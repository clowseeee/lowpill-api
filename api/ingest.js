const { createClient } = require('@supabase/supabase-js');
const { z } = require('zod');
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE);

const schema = z.object({
  company: z.string(),
  source: z.object({
    url: z.string(),
    title: z.string(),
    doc_type: z.string().optional(),
    date: z.string().optional(),
  }),
  facts: z.array(z.object({
    as_of_date: z.string().optional(),
    domain: z.string().optional(),
    metric_key: z.string(),
    metric_value: z.string(),
    unit: z.string().optional(),
    qualifier: z.string().optional(),
    source_quote: z.string().optional(),
    extraction_confidence: z.number().optional(),
  })).optional(),
  events: z.array(z.object({
    date: z.string().optional(),
    type: z.string().optional(),
    summary: z.string().optional(),
    impacted_metrics: z.string().optional(),
    source_quote: z.string().optional(),
    confidence: z.number().optional(),
  })).optional(),
});

module.exports = async (req, res) => {

  // Authorization guard
  if (req.headers.authorization !== `Bearer ${process.env.INGEST_TOKEN}`) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  try {
    if (req.method === 'GET') return res.status(200).send('pong');

    const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
    const parsed = schema.parse(body);

    const SUPABASE_URL = process.env.SUPABASE_URL;
    const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
      return res.status(500).json({ error: 'Missing Supabase env vars' });
    }

    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE);

    // Company
    const slug = parsed.company.toLowerCase();
    const { data: existingCompany, error: cSelErr } = await supabase
      .from('companies').select('*').eq('slug', slug).maybeSingle();
    if (cSelErr) return res.status(500).json({ error: cSelErr.message });

    let company = existingCompany;
    if (!company) {
      const { data, error } = await supabase
        .from('companies').insert({ slug, name: parsed.company }).select().single();
      if (error) return res.status(500).json({ error: error.message });
      company = data;
    }

    // Source doc
    const { data: src, error: sErr } = await supabase
      .from('source_docs').insert({
        company_id: company.id,
        url: parsed.source.url,
        title: parsed.source.title,
        doc_type: parsed.source.doc_type ?? null,
        doc_date: parsed.source.date ? new Date(parsed.source.date) : null,
      }).select().single();
    if (sErr) return res.status(500).json({ error: sErr.message });

    // Facts
    if (parsed.facts?.length) {
      const rows = parsed.facts.map(f => ({
        company_id: company.id,
        source_id: src.id,
        as_of_date: f.as_of_date ? new Date(f.as_of_date) : null,
        domain: f.domain ?? null,
        metric_key: f.metric_key,
        metric_value: f.metric_value,
        unit: f.unit ?? null,
        qualifier: f.qualifier ?? null,
        source_quote: f.source_quote ?? null,
        extraction_confidence: f.extraction_confidence ?? null,
      }));
      const { error } = await supabase.from('facts').insert(rows);
      if (error) return res.status(500).json({ error: error.message });
    }

    // Events
    if (parsed.events?.length) {
      const rows = parsed.events.map(e => ({
        company_id: company.id,
        source_id: src.id,
        event_date: e.date ? new Date(e.date) : null,
        type: e.type ?? null,
        summary: e.summary ?? null,
        impacted_metrics: e.impacted_metrics ?? null,
        source_quote: e.source_quote ?? null,
        confidence: e.confidence ?? null,
      }));
      const { error } = await supabase.from('events').insert(rows);
      if (error) return res.status(500).json({ error: error.message });
    }

    return res.status(200).json({ ok: true, company: slug, source_id: src.id });
  } catch (err) {
    console.error('INGEST ERROR:', err?.message || err);
    return res.status(500).json({ error: err?.message || 'unknown' });
  }
};

