// /api/ingest.js — Lowpill v1.9.3
// Batch dictionary upsert, optional DB-level dedup, stricter slug, 405 handling
// + robust parseNumeric (FR/US, k/m/b, (neg), symbols) + currency guess
// + constant-time auth + insights/news hash dedup + facts text-hash dedup for non-numeric
// + PATCHES: z.coerce.string() for metric_value, parseNumeric removes prefixed currency codes

const { createClient } = require('@supabase/supabase-js');
const { z } = require('zod');
const crypto = require('crypto');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const INGEST_TOKEN = process.env.INGEST_TOKEN;

// If you created unique indexes in DB, set this to true to skip per-row preselect dedup
const USE_DB_DEDUP = true;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE);

// ---------- utils ----------
const toSlug = (s) => {
  if (!s) return '';
  return String(s)
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '') // strip diacritics
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
};

const md5 = (s) => crypto.createHash('md5').update(s || '').digest('hex');

function constTimeEq(a = '', b = '') {
  const A = Buffer.from(String(a));
  const B = Buffer.from(String(b));
  if (A.length !== B.length) return false;
  return crypto.timingSafeEqual(A, B);
}

function safeDate(input) {
  if (!input) return null;
  const t = new Date(input);
  return Number.isNaN(t.getTime()) ? null : t;
}

// parseNumeric robuste FR/US, suffixes, négatifs (parenthèses), devise, multiplicateurs, x
function parseNumeric(x) {
  if (x == null) return null;
  let s = String(x).trim();

  // signes, espaces, % et x ; harmonise tirets
  s = s.replace(/[％%]/g, '')
       .replace(/\u00A0/g, '')
       .replace(/\s/g, '')
       .replace(/[–—−]/g, '-')
       .replace(/x$/i, '');

  // négatifs style (1,234) ou (1.234,56)
  const isNeg = /^\(.*\)$/.test(s);
  if (isNeg) s = s.slice(1, -1);

  // FR décimal (1.234,56) vs US (1,234.56)
  if (/[0-9],[0-9]{1,3}$/.test(s) && !/\.[0-9]{1,3}$/.test(s)) {
    s = s.replace(/\./g, '').replace(',', '.'); // FR
  } else {
    s = s.replace(/,/g, ''); // US
  }

  // suffixes: k/m/b (anglais) & Md/Mds (FR milliards)
  let mul = 1;
  if (/([kK])$/.test(s)) { mul = 1e3; s = s.replace(/k$/i, ''); }
  else if (/([mM])$/.test(s)) { mul = 1e6; s = s.replace(/m$/i, ''); }
  else if (/([bB])$/.test(s)) { mul = 1e9; s = s.replace(/b$/i, ''); }
  else if (/Mds?$/.test(s)) { mul = 1e9; s = s.replace(/Mds?$/,''); }
  else if (/Md?$/.test(s)) { mul = 1e9; s = s.replace(/Md?$/,''); }

  // symbols/codes currency aux extrémités (PATCH: gère aussi USD 1,234 etc.)
  s = s
    .replace(/^(€|\$|£|¥|RMB|USD|EUR|GBP|JPY|CNY)\s*/i, '')
    .replace(/\s*(USD|EUR|GBP|JPY|CNY)$/i, '');

  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  const num = n * mul * (isNeg ? -1 : 1);
  return Number.isFinite(num) ? num : null;
}

function norm01(v) {
  if (v == null) return null;
  let x = Number(v);
  if (!Number.isFinite(x)) return null;
  if (x > 1.0001) x = x / 100; // tolère 95 -> 0.95
  if (x < 0) x = 0;
  if (x > 1) x = 1;
  return x;
}

function mapThemeEnum(t) {
  if (!t) return 'other';
  const m = String(t).toLowerCase();
  if (['growth','croissance'].includes(m)) return 'growth';
  if (['margin','marge','profitability'].includes(m)) return 'margin';
  if (['risk','risque'].includes(m)) return 'risk';
  if (['cash','cashflow','flux'].includes(m)) return 'cash';
  if (['strategy','stratégie','strategie'].includes(m)) return 'strategy';
  if (['geography','geo','china','europe','us'].includes(m)) return 'geography';
  if (['esg','sustainability'].includes(m)) return 'esg';
  if (['product','produit','pipeline'].includes(m)) return 'product';
  if (['moat','avantage','barrier'].includes(m)) return 'moat';
  return 'other';
}

function mapDocTypeEnum(t) {
  if (!t) return 'other';
  const m = String(t).toLowerCase();
  if (m.includes('annual')) return 'annual_report';
  if (m.includes('quarter')) return 'quarterly_report';
  if (m.includes('press')) return 'press_release';
  if (m.includes('presentation')) return 'investor_presentation';
  if (m.includes('news')) return 'news';
  if (m.includes('web') || m.includes('site')) return 'webpage';
  return 'other';
}

function guessCurrencyCode(str='') {
  const s = String(str).toUpperCase();
  if (/[€]/.test(s) || /EUR/.test(s)) return 'EUR';
  if (/USD|\$/.test(s)) return 'USD';
  if (/GBP|£/.test(s)) return 'GBP';
  if (/JPY|¥/.test(s)) return 'JPY';
  if (/CNY|RMB/.test(s)) return 'CNY';
  return 'CURRENCY_UNKNOWN';
}

// ---------- provenance ----------
const PROVENANCE_RULES = [
  { pattern: /(^|\.)lvmh\.com$/i,         type: 'issuer',    name: 'LVMH',          trust: 0.70 },
  { pattern: /(^|\.)sec\.gov$/i,          type: 'regulator', name: 'SEC',           trust: 0.95 },
  { pattern: /(^|\.)amf-france\.org$/i,   type: 'regulator', name: 'AMF',           trust: 0.95 },
  { pattern: /(^|\.)euronext\.com$/i,     type: 'exchange',  name: 'Euronext',      trust: 0.90 },
  { pattern: /(^|\.)businesswire\.com$/i, type: 'newswire',  name: 'BusinessWire',  trust: 0.80 },
  { pattern: /(^|\.)prnewswire\.com$/i,   type: 'newswire',  name: 'PR Newswire',   trust: 0.80 },
  { pattern: /(^|\.)reuters\.com$/i,      type: 'media',     name: 'Reuters',       trust: 0.85 },
  { pattern: /(^|\.)bloomberg\.com$/i,    type: 'media',     name: 'Bloomberg',     trust: 0.85 },
  { pattern: /(^|\.)seekingalpha\.com$/i, type: 'analyst',   name: 'Seeking Alpha', trust: 0.70 },
];

function classifyProvenance(url) {
  try {
    const host = new URL(url).hostname.toLowerCase();
    for (const r of PROVENANCE_RULES) {
      if (r.pattern.test(host)) {
        return {
          publisher_domain: host,
          publisher_name:   r.name,
          publisher_type:   r.type, // issuer | regulator | exchange | newswire | analyst | media | other
          is_official:      ['issuer','regulator','exchange'].includes(r.type),
          trust_score:      r.trust
        };
      }
    }
    return { publisher_domain: host, publisher_name: host, publisher_type: 'other', is_official: false, trust_score: 0.50 };
  } catch {
    return { publisher_domain: null, publisher_name: null, publisher_type: 'other', is_official: false, trust_score: 0.50 };
  }
}

// ---------- schema ----------
const schema = z.object({
  company: z.string().min(1),
  source: z.object({
    url: z.string().url(),
    title: z.string().min(1),
    doc_type: z.string().optional(),
    published_at: z.string().optional(),
    doc_language: z.string().optional(),
    version: z.number().int().optional(),
    source_md5: z.string().optional()
  }),
  facts: z.array(z.object({
    as_of_date: z.string().optional(),
    domain: z.string().optional(),              // ex: 'income_statement' | 'balance_sheet' | 'cash_flow_statement'
    metric_key: z.string().min(1),
    metric_value: z.coerce.string().min(1),     // PATCH: accept numbers as input, coerce to string
    unit: z.string().optional(),
    qualifier: z.string().optional(),
    source_quote: z.string().optional(),
    extraction_confidence: z.number().optional(),
    impact_score: z.number().optional(),

    // compat blueprint
    display_label: z.string().optional(),
    framework_bucket: z.enum(['competitiveness','solvency','development']).optional(),
    primary_source: z.enum(['income_statement','balance_sheet','cash_flow_statement']).optional(),
  })).optional(),
  insights: z.array(z.object({
    theme: z.string().optional(),
    text: z.string().min(1),
    confidence: z.number().optional()
  })).optional(),
  news: z.array(z.object({
    event_date: z.string().optional(),
    headline: z.string().min(1),
    summary: z.string().optional(),
    full_text: z.string().optional(),
    theme: z.string().optional(),
    importance: z.number().optional()
  })).optional()
});

// ---------- helpers ----------
async function getOrCreateCompany(name) {
  const slug = toSlug(name);
  const { data, error } = await supabase
    .from('companies')
    .upsert({ slug, name }, { onConflict: 'slug' })
    .select()
    .single();
  if (error) throw new Error(`company upsert: ${error.message}`);
  return data;
}

async function getOrCreateSource({
  company_id, url, title, source_type, published_at, doc_language, version, source_md5, provenance
}) {
  const sel1 = await supabase
    .from('sources')
    .select('*')
    .eq('company_id', company_id)
    .eq('url', url)
    .order('published_at', { ascending: false, nullsFirst: false })
    .limit(1)
    .maybeSingle();
  if (sel1.error) throw new Error(`source select: ${sel1.error.message}`);
  if (sel1.data) return sel1.data;

  const ins = await supabase
    .from('sources')
    .insert({
      company_id,
      url,
      title,
      source_type,
      published_at,
      source_md5,
      doc_language,
      version,
      publisher_domain: provenance.publisher_domain,
      publisher_name:   provenance.publisher_name,
      publisher_type:   provenance.publisher_type,
      is_official:      provenance.is_official,
      trust_score:      provenance.trust_score
    })
    .select()
    .single();

  if (ins.error) {
    const sel2 = await supabase
      .from('sources')
      .select('*')
      .eq('company_id', company_id)
      .eq('url', url)
      .order('published_at', { ascending: false, nullsFirst: false })
      .limit(1)
      .maybeSingle();
    if (sel2.error || !sel2.data) {
      throw new Error(`source insert: ${ins.error.message}`);
    }
    return sel2.data;
  }
  return ins.data;
}

async function batchUpsertMetricsDictionary(facts) {
  if (!facts?.length) return new Map();

  const unique = new Map();
  for (const f of facts) {
    const key_slug = toSlug(f.metric_key);
    if (!unique.has(key_slug)) {
      const payload = {
        key_slug,
        label: f.display_label || f.metric_key,
      };
      if (f.framework_bucket) payload.framework_bucket = toSlug(f.framework_bucket);
      if (f.primary_source)   payload.primary_source   = toSlug(f.primary_source);
      else if (f.domain)      payload.primary_source   = toSlug(f.domain);
      if (f.display_label)    payload.display_label    = f.display_label;
      unique.set(key_slug, payload);
    }
  }

  const payloads = Array.from(unique.values());
  if (!payloads.length) return new Map();

  const { data, error } = await supabase
    .from('metrics_dictionary')
    .upsert(payloads, { onConflict: 'key_slug' })
    .select('id,key_slug');

  if (error) throw new Error(`metrics_dictionary batch upsert: ${error.message}`);

  const map = new Map();
  for (const row of data) map.set(row.key_slug, row.id);
  return map;
}

// ---------- handler ----------
module.exports = async (req, res) => {
  try {
    // Auth (constant-time) + method
    const authHeader = req.headers.authorization || req.headers.Authorization || '';
    const token = (authHeader || '').split(/\s+/)[1] || '';
    if (!INGEST_TOKEN || !constTimeEq(token, INGEST_TOKEN)) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
    if (req.method === 'GET') return res.status(200).send('pong');
    if (req.method !== 'POST') return res.status(405).json({ error: 'Method Not Allowed' });

    // Parse input (robuste aux bodies stringifiés)
    let body;
    try {
      body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
    } catch (e) {
      return res.status(400).json({ error: 'Invalid JSON body' });
    }
    const parsed = schema.parse(body);

    // Company
    const company = await getOrCreateCompany(parsed.company);

    // Source
    const source_type  = mapDocTypeEnum(parsed.source.doc_type);
    const published_at = safeDate(parsed.source.published_at);
    const source_md5   = parsed.source.source_md5 || null;
    const doc_language = (parsed.source.doc_language || '').toLowerCase() || null;
    const version      = parsed.source.version ?? 1;
    const provenance   = classifyProvenance(parsed.source.url);

    const source = await getOrCreateSource({
      company_id: company.id,
      url: parsed.source.url,
      title: parsed.source.title,
      source_type,
      published_at,
      doc_language,
      version,
      source_md5,
      provenance
    });

    // Canonical IDs
    const sourceId   = source.id;
    const trustScore = source.trust_score ?? provenance.trust_score ?? 0.5;

    // ---------- Facts (batched) ----------
    if (parsed.facts?.length) {
      // 1) batch upsert dictionary and build key_slug -> id map
      const dictMap = await batchUpsertMetricsDictionary(parsed.facts);

      // 2) build rows
      const factRows = [];
      for (const f of parsed.facts) {
        const asOfRaw = safeDate(f.as_of_date) || published_at || null;
        const asOfISO = asOfRaw ? asOfRaw.toISOString() : null;

        const mvn  = parseNumeric(f.metric_value);

        // unité heuristique: % ou devise ou null
        const valueStr = String(f.metric_value || '');
        const cur = guessCurrencyCode(valueStr);
        const unit =
          f.unit ??
          (/%/.test(valueStr) ? '%' :
           (cur !== 'CURRENCY_UNKNOWN' ? cur : null));

        const key_slug = toSlug(f.metric_key);
        const metric_id = dictMap.get(key_slug);
        if (!metric_id) continue; // sécurité

        // Optional pre-select dedup if DB unique index not enabled
        if (!USE_DB_DEDUP && asOfISO && mvn != null) {
          const { data: dup } = await supabase
            .from('facts')
            .select('id')
            .eq('company_id', company.id)
            .eq('metric_id',  metric_id)
            .eq('as_of_date', asOfISO)
            .eq('metric_value_num', mvn)
            .limit(1)
            .maybeSingle();
          if (dup) continue;
        }

        // hash texte pour dédup non-numérique
        const fact_md5 = md5(`${key_slug}|${asOfISO||''}|${String(f.metric_value)}`);

        factRows.push({
          company_id: company.id,
          source_id:  sourceId,
          as_of_date: asOfISO,
          domain:     f.domain || null,
          metric_key: f.metric_key,
          metric_id,
          metric_value: f.metric_value,
          metric_value_num: mvn,
          unit,
          qualifier:  f.qualifier || null,
          source_quote: f.source_quote || null,
          extraction_confidence: norm01(f.extraction_confidence),
          impact_score:          norm01(f.impact_score) ?? 0,
          fact_md5
        });
      }

      if (factRows.length) {
        const { error: fErr } = await supabase.from('facts').insert(factRows);
        if (fErr && fErr.code !== '23505') {
          return res.status(500).json({
            error: `facts insert: ${fErr.message}`,
            debug: { sourceIdUsed: factRows?.[0]?.source_id, companyId: company.id }
          });
        }
      }
    }

    // ---------- Insights (avec hash dédup) ----------
    if (parsed.insights?.length) {
      const seen = new Set();
      const insightRows = [];
      for (const i of parsed.insights) {
        const conf = norm01(i.confidence) ?? 0.8;
        const text = i.text;
        const text_md5 = md5(text.trim());
        if (!USE_DB_DEDUP && seen.has(text_md5)) continue;
        seen.add(text_md5);
        insightRows.push({
          company_id: company.id,
          source_id:  sourceId,
          theme_enum: mapThemeEnum(i.theme),
          theme:      i.theme || null,
          text,
          text_md5,
          confidence: conf,
          provenance_score: conf * trustScore
        });
      }
      if (insightRows.length) {
        const { error: iErr } = await supabase.from('insights').insert(insightRows);
        if (iErr && iErr.code !== '23505') {
          return res.status(500).json({
            error: `insights insert: ${iErr.message}`,
            debug: { sourceIdUsed: insightRows?.[0]?.source_id, companyId: company.id }
          });
        }
      }
    }

    // ---------- News (déjà hashées) ----------
    if (parsed.news?.length) {
      const newsRows = [];
      for (const n of parsed.news) {
        const textBlob = `${n.headline || ''}\n${n.summary || ''}\n${n.full_text || ''}`;
        const text_md5 = md5(textBlob);

        if (!USE_DB_DEDUP) {
          const { data: dup } = await supabase
            .from('news_events')
            .select('id')
            .eq('company_id', company.id)
            .eq('source_id', sourceId)
            .eq('text_md5', text_md5)
            .maybeSingle();
          if (dup) continue;
        }

        newsRows.push({
          company_id: company.id,
          source_id: sourceId,
          event_date: safeDate(n.event_date)?.toISOString() || (published_at?.toISOString()) || new Date().toISOString(),
          headline: n.headline,
          summary: n.summary || null,
          full_text: n.full_text ? n.full_text.slice(0, 8000) : null, // 8k chars cap
          theme_enum: mapThemeEnum(n.theme),
          importance: norm01(n.importance) ?? 0.6,
          text_md5
        });
      }
      if (newsRows.length) {
        const { error: nErr } = await supabase.from('news_events').insert(newsRows);
        if (nErr && nErr.code !== '23505') {
          return res.status(500).json({ error: `news insert: ${nErr.message}` });
        }
      }
    }

    return res.status(200).json({ ok: true, company: toSlug(parsed.company), source_id: sourceId });
  } catch (err) {
    console.error('INGEST ERROR:', err);
    return res.status(500).json({ error: err?.message || 'unknown' });
  }
};
