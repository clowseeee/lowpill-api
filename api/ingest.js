// /api/ingest.js — Lowpill v1.7 (idempotent, FK-safe, provenance-aware + news)
const { createClient } = require('@supabase/supabase-js');
const { z } = require('zod');
const crypto = require('crypto');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const INGEST_TOKEN = process.env.INGEST_TOKEN;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE);

// ---------- utils ----------
const toSlug = (s) => (s || '').trim().toLowerCase();
const md5 = (s) => crypto.createHash('md5').update(s || '').digest('hex');

function safeDate(input) {
  if (!input) return null;
  const t = new Date(input);
  return isNaN(t.getTime()) ? null : t;
}
function parseNumeric(x) {
  if (x == null) return null;
  let s = String(x).trim();
  s = s.replace(/%/g, '').replace(/\s/g, '').replace(/,/g, '');
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
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
  if (['strategy','stratégie'].includes(m)) return 'strategy';
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
          publisher_type:   r.type,
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
    domain: z.string().optional(),
    metric_key: z.string().min(1),
    metric_value: z.string().min(1),
    unit: z.string().optional(),
    qualifier: z.string().optional(),
    source_quote: z.string().optional(),
    extraction_confidence: z.number().optional(),
    impact_score: z.number().optional()
  })).optional(),
  insights: z.array(z.object({
    theme: z.string().optional(),
    text: z.string().min(1),
    confidence: z.number().optional()
  })).optional(),
  // 🆕 News supportée par l’API
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

// ---------- handler ----------
module.exports = async (req, res) => {
  try {
    if (!INGEST_TOKEN || req.headers.authorization !== `Bearer ${INGEST_TOKEN}`) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
    if (req.method === 'GET') return res.status(200).send('pong');

    const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
    const parsed = schema.parse(body);

    // Company
    const company = await getOrCreateCompany(parsed.company);

    // Source
    const source_type  = mapDocTypeEnum(parsed.source.doc_type);
    const published_at = safeDate(parsed.source.published_at);
    const source_md5   = parsed.source.source_md5 || null;
    const doc_language = parsed.source.doc_language || null;
    const version      = parsed.source.version ?? 1;
    const provenance   = classifyProvenance(parsed.source.url);

    const sourceRow = await getOrCreateSource({
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

    const { data: srcCheck, error: srcCheckErr } = await supabase
      .from('sources')
      .select('id, trust_score')
      .eq('company_id', company.id)
      .eq('url', parsed.source.url)
      .maybeSingle();
    if (srcCheckErr || !srcCheck) {
      return res.status(500).json({ error: 'source lookup failed before inserts' });
    }
    const sourceId   = srcCheck.id;
    const trustScore = srcCheck.trust_score ?? provenance.trust_score ?? 0.5;

    // Facts
    if (parsed.facts?.length) {
      const factRows = [];
      for (const f of parsed.facts) {
        const key_slug = toSlug(f.metric_key);
        const dict = await supabase
          .from('metrics_dictionary')
          .upsert({ key_slug, label: f.metric_key }, { onConflict: 'key_slug' })
          .select()
          .single();
        if (dict.error) return res.status(500).json({ error: `metrics_dictionary upsert: ${dict.error.message}` });

        factRows.push({
          company_id: company.id,
          source_id: sourceId, // FK-safe
          as_of_date: safeDate(f.as_of_date),
          domain: f.domain || null,
          metric_key: f.metric_key,
          metric_id: dict.data.id,
          metric_value: f.metric_value,
          metric_value_num: parseNumeric(f.metric_value),
          unit: f.unit || null,
          qualifier: f.qualifier || null,
          source_quote: f.source_quote || null,
          extraction_confidence: norm01(f.extraction_confidence),
          impact_score: norm01(f.impact_score) ?? 0
        });
      }
      const { error: fErr } = await supabase.from('facts').insert(factRows);
      if (fErr && fErr.code !== '23505') {
        return res.status(500).json({ error: `facts insert: ${fErr.message}`, debug: { sourceIdUsed: factRows?.[0]?.source_id, companyId: company.id } });
      }
    }

    // Insights (pondérés par la provenance)
    if (parsed.insights?.length) {
      const insightRows = parsed.insights.map(i => {
        const conf = norm01(i.confidence) ?? 0.8;
        return {
          company_id: company.id,
          source_id:  sourceId, // FK-safe
          theme_enum: mapThemeEnum(i.theme),
          theme:      i.theme || null,
          text:       i.text,
          confidence: conf,
          provenance_score: conf * trustScore
        };
      });
      const { error: iErr } = await supabase.from('insights').insert(insightRows);
      if (iErr && iErr.code !== '23505') {
        return res.status(500).json({ error: `insights insert: ${iErr.message}`, debug: { sourceIdUsed: insightRows?.[0]?.source_id, companyId: company.id } });
      }
    }

    // 🆕 News
    if (parsed.news?.length) {
      for (const n of parsed.news) {
        const textBlob = `${n.headline || ''}\n${n.summary || ''}\n${n.full_text || ''}`;
        const text_md5 = md5(textBlob);

        const { data: dup } = await supabase
          .from('news_events')
          .select('id')
          .eq('company_id', company.id)
          .eq('source_id', sourceId)
          .eq('text_md5', text_md5)
          .maybeSingle();
        if (dup) continue;

        const { error: nErr } = await supabase.from('news_events').insert({
          company_id: company.id,
          source_id: sourceId,
          event_date: safeDate(n.event_date) || published_at || new Date(),
          headline: n.headline,
          summary: n.summary || null,
          full_text: n.full_text || null,
          theme_enum: mapThemeEnum(n.theme),
          importance: norm01(n.importance) ?? 0.6,
          text_md5
        });
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
