// /api/ingest.js  (Vercel serverless - CommonJS)
const { createClient } = require('@supabase/supabase-js');
const { z } = require('zod');
const crypto = require('crypto');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE);

// --- Helpers ---
const toSlug = (s) => s.trim().toLowerCase();
const md5 = (s) => crypto.createHash('md5').update(s || '').digest('hex');

// "22.4%" -> 22.4 ; "1,234.56" -> 1234.56 ; otherwise null
function parseNumeric(x) {
  if (x == null) return null;
  let s = String(x).trim();
  const hasPct = s.endsWith('%');
  s = s.replace(/%/g, '').replace(/\s/g, '').replace(/,/g, '');
  const n = Number(s);
  if (Number.isFinite(n)) return hasPct ? n : n; // laisse la sémantique au 'unit'
  return null;
}

// Force [0..1]; accepte aussi 0..100
function norm01(v) {
  if (v == null) return null;
  let x = Number(v);
  if (!Number.isFinite(x)) return null;
  if (x > 1.0001) x = x / 100; // 73 -> 0.73
  if (x < 0) x = 0;
  if (x > 1) x = 1;
  return x;
}

// Map text -> enum theme (fallback 'other')
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

// Map doc_type text -> enum doc_type_enum
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

// --- Zod schema aligned with v1.6 ---
const schema = z.object({
  company: z.string().min(1),
  source: z.object({
    url: z.string().url(),
    title: z.string().min(1),
    doc_type: z.string().optional(),         // mapped -> source_type enum
    published_at: z.string().optional(),     // ISO date
    doc_language: z.string().optional(),     // 'en','fr',...
    version: z.number().int().optional(),
    source_md5: z.string().optional()        // allow upstream to send md5 of raw text (best)
  }),
  facts: z.array(z.object({
    as_of_date: z.string().optional(),
    domain: z.string().optional(),
    metric_key: z.string().min(1),
    metric_value: z.string().min(1),         // text form "22.4%"
    unit: z.string().optional(),             // "%","EUR", etc.
    qualifier: z.string().optional(),
    source_quote: z.string().optional(),
    extraction_confidence: z.number().optional(), // 0..1 or 0..100
    impact_score: z.number().optional()            // 0..1 or 0..100
  })).optional(),
  insights: z.array(z.object({
    theme: z.string().optional(),
    text: z.string().min(1),
    confidence: z.number().optional()        // 0..1 or 0..100
  })).optional()
});

module.exports = async (req, res) => {
  // Auth
  if (req.headers.authorization !== `Bearer ${process.env.INGEST_TOKEN}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  if (req.method === 'GET') return res.status(200).send('pong');

  try {
    const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
    const parsed = schema.parse(body);

    // 1) Company upsert by slug
    const slug = toSlug(parsed.company);
    const { data: company, error: cErr } = await supabase
      .from('companies')
      .upsert({ slug, name: parsed.company }, { onConflict: 'slug' })
      .select()
      .single();
    if (cErr) return res.status(500).json({ error: `company upsert: ${cErr.message}` });

    // 2) Source insert with unique constraint handling
    const source_md5 = parsed.source.source_md5 || null; // idéalement MD5 du texte source
    const source_type = mapDocTypeEnum(parsed.source.doc_type);
    const published_at = parsed.source.published_at ? new Date(parsed.source.published_at) : null;
    const doc_language = parsed.source.doc_language || null;
    const version = parsed.source.version ?? 1;

    let sourceRow;
    // On essaie d'insérer; si unique_violation (23505), on SELECT l’existante (via URL + md5 + company)
    const insertSource = await supabase
      .from('sources')
      .insert({
        company_id: company.id,
        url: parsed.source.url,
        title: parsed.source.title,
        source_type,
        published_at,
        source_md5,
        doc_language,
        version
      })
      .select()
      .single();

    if (insertSource.error) {
      // Si conflit unique => récupérer la ligne existante
      if (insertSource.error.code === '23505') {
        const { data: existing, error: sSelErr } = await supabase
          .from('sources')
          .select('*')
          .eq('company_id', company.id)
          .eq('url', parsed.source.url)
          // Pas possible de filtrer sur l'expression coalesce dans un eq simple; on fait deux cas :
          .or(`source_md5.eq.${source_md5 || ''},and(source_md5.is.null)`)
          .order('published_at', { ascending: false, nullsFirst: false })
          .limit(1)
          .maybeSingle();
        if (sSelErr || !existing) {
          return res.status(500).json({ error: `source select after conflict: ${sSelErr?.message || 'not found'}` });
        }
        sourceRow = existing;
      } else {
        return res.status(500).json({ error: `source insert: ${insertSource.error.message}` });
      }
    } else {
      sourceRow = insertSource.data;
    }

    // 3) Facts (optional)
    if (parsed.facts?.length) {
      const factRows = [];
      for (const f of parsed.facts) {
        // metrics_dictionary: ensure metric_key exists
        const key_slug = toSlug(f.metric_key);
        const mdict = await supabase
          .from('metrics_dictionary')
          .upsert({ key_slug, label: f.metric_key }, { onConflict: 'key_slug' })
          .select()
          .single();
        if (mdict.error) {
          return res.status(500).json({ error: `metrics_dictionary upsert: ${mdict.error.message}` });
        }

        factRows.push({
          company_id: company.id,
          source_id: sourceRow.id,
          as_of_date: f.as_of_date ? new Date(f.as_of_date) : null,
          domain: f.domain || null,
          metric_key: f.metric_key,
          metric_id: mdict.data.id,
          metric_value: f.metric_value,
          metric_value_num: parseNumeric(f.metric_value),
          unit: f.unit || null,
          qualifier: f.qualifier || null,
          source_quote: f.source_quote || null,
          extraction_confidence: norm01(f.extraction_confidence),
          impact_score: norm01(f.impact_score) ?? 0
        });
      }

      // insert facts (la contrainte uq_facts_dedup évite les doublons exacts)
      const { error: fErr } = await supabase.from('facts').insert(factRows);
      if (fErr && fErr.code !== '23505') {
        // 23505 = unique violation -> on ignore pour idempotence
        return res.status(500).json({ error: `facts insert: ${fErr.message}` });
      }
    }

    // 4) Insights (optional)
    if (parsed.insights?.length) {
      const insightRows = parsed.insights.map((i) => ({
        company_id: company.id,
        source_id: sourceRow.id,
        theme_enum: mapThemeEnum(i.theme),
        theme: i.theme || null,
        text: i.text,
        confidence: norm01(i.confidence)
        // text_md5 est rempli par le trigger
        // embedding sera rempli plus tard par ta fonction async (Edge Function)
      }));

      const { error: iErr } = await supabase.from('insights').insert(insightRows);
      if (iErr && iErr.code !== '23505') {
        return res.status(500).json({ error: `insights insert: ${iErr.message}` });
      }
    }

    return res.status(200).json({ ok: true, company: slug, source_id: sourceRow.id });
  } catch (err) {
    console.error('INGEST ERROR:', err);
    return res.status(500).json({ error: err?.message || 'unknown' });
  }
};
