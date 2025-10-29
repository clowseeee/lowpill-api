// /api/ingest.js — Lowpill v1.6 (robuste, idempotent)
const { createClient } = require('@supabase/supabase-js');
const { z } = require('zod');
const crypto = require('crypto');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const INGEST_TOKEN = process.env.INGEST_TOKEN;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE);

// --------- utils ----------
const toSlug = (s) => s.trim().toLowerCase();
const md5 = (s) => crypto.createHash('md5').update(s || '').digest('hex');

function parseNumeric(x) {
  if (x == null) return null;
  let s = String(x).trim();
  s = s.replace(/%/g, '').replace(/\s/g, '').replace(/,/g, '');
  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  return n;
}

function norm01(v) {
  if (v == null) return null;
  let x = Number(v);
  if (!Number.isFinite(x)) return null;
  if (x > 1.0001) x = x / 100;
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

// --------- schema ----------
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
  })).optional()
});

// --------- helper: get or create Source ----------
async function getOrCreateSource(supabase, { companyId, url, title, source_type, published_at, doc_language, version, source_md5 }) {
  // 1) chercher d'abord par (company_id, url)
  const { data: found, error: selErr } = await supabase
    .from('sources')
    .select('*')
    .eq('company_id', companyId)
    .eq('url', url)
    .order('published_at', { ascending: false, nullsFirst: false })
    .limit(1)
    .maybeSingle();

  if (selErr) throw new Error(`source select: ${selErr.message}`);
  if (found) return found;

  // 2) sinon insérer
  const ins = await supabase
    .from('sources')
    .insert({
      company_id: companyId,
      url,
      title,
      source_type,
      published_at,
      source_md5,
      doc_language,
      version
    })
    .select()
    .single();

  if (ins.error) throw new Error(`source insert: ${ins.error.message}`);
  return ins.data;
}

// --------- handler ----------
module.exports = async (req, res) => {
  try {
    // Auth
    if (!INGEST_TOKEN || req.headers.authorization !== `Bearer ${INGEST_TOKEN}`) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // GET = ping
    if (req.method === 'GET') return res.status(200).send('pong');

    // Parse
    const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
    const parsed = schema.parse(body);

    // Company
    const slug = toSlug(parsed.company);
    const { data: company, error: cErr } = await supabase
      .from('companies')
      .upsert({ slug, name: parsed.company }, { onConflict: 'slug' })
      .select()
      .single();
    if (cErr) return res.status(500).json({ error: `company upsert: ${cErr.message}` });

    // Source fields
    const source_md5 = parsed.source.source_md5 || null;
    const source_type = mapDocTypeEnum(parsed.source.doc_type);
    const published_at = parsed.source.published_at ? new Date(parsed.source.published_at) : null;
    const doc_language = parsed.source.doc_language || null;
    const version = parsed.source.version ?? 1;

    // Source (robuste, idempotent)
    const sourceRow = await getOrCreateSource(supabase, {
      companyId: company.id,
      url: parsed.source.url,
      title: parsed.source.title,
      source_type,
      published_at,
      doc_language,
      version,
      source_md5
    });
// --- Safety check: ensure sourceRow.id truly exists in public.sources
{
  const { data: exists, error: exErr } = await supabase
    .from('sources')
    .select('id')
    .eq('id', sourceRow.id)
    .maybeSingle();

  if (exErr) {
    return res.status(500).json({ error: `source verify: ${exErr.message}` });
  }

  if (!exists) {
    // Fallback: re-fetch by (company_id, url) and override id
    const { data: fallback, error: fbErr } = await supabase
      .from('sources')
      .select('id')
      .eq('company_id', company.id)
      .eq('url', parsed.source.url)
      .order('published_at', { ascending: false, nullsFirst: false })
      .limit(1)
      .maybeSingle();

    if (fbErr || !fallback) {
      return res.status(500).json({ error: 'source id not found after insert/select' });
    }
    sourceRow.id = fallback.id; // <— on force l’ID existant en base
  }
} 
// --- ALWAYS resolve a canonical sourceId from DB by (company_id, url)
const { data: srcCheck, error: srcCheckErr } = await supabase
  .from('sources')
  .select('id')
  .eq('company_id', company.id)
  .eq('url', parsed.source.url)
  .maybeSingle();

if (srcCheckErr || !srcCheck) {
  return re
s.status(500).json({ error: `source lookup failed before inserts` });
}
const sourceId = srcCheck.id;

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
          source_id: sourceId,
          as_of_date: f.as_of_date ? new Date(f.as_of_date) : null,
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
      if (fErr && fErr.code !== '23505') return res.status(500).json({ error: `facts insert: ${fErr.message}` });
    }

    // Insights
    if (parsed.insights?.length) {
      const insightRows = parsed.insights.map(i => ({
        company_id: company.id,
        source_id: sourceId,
        theme_enum: mapThemeEnum(i.theme),
        theme: i.theme || null,
        text: i.text,
        confidence: norm01(i.confidence)
      }));
      const { error: iErr } = await supabase.from('insights').insert(insightRows);
      if (iErr && iErr.code !== '23505') return res.status(500).json({ error: `insights insert: ${iErr.message}` });
    }

    return res.status(200).json({ ok: true, company: slug, source_id: sourceId });
  } catch (err) {
    console.error('INGEST ERROR:', err);
    return res.status(500).json({ error: err?.message || 'unknown' });
  }
};
