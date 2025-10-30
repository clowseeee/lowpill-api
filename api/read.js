// /api/read.js — retourne un payload “produit” pour Willo + Front
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE);

module.exports = async (req, res) => {
  try {
    const { company, metric, limit = 8 } = req.query || {};
    if (!company) {
      return res.status(400).json({ error: 'Missing ?company=<slug or name>' });
    }

    // 1) Résoudre le slug → id/slug
    const { data: comp, error: cErr } = await supabase
      .from('companies')
      .select('id, slug, name, domain')
      .or(`slug.eq.${company},name.ilike.%${company}%`)
      .limit(1)
      .maybeSingle();
    if (cErr) return res.status(500).json({ error: cErr.message });
    if (!comp) return res.status(404).json({ error: 'Company not found' });

    // 2) Facts analytiques (plus)
    let q = supabase
      .from('fact_analysis_plus')
      .select('*')
      .eq('company_slug', comp.slug)
      .order('as_of_date', { ascending: false })
      .limit(limit);

    if (metric) q = q.eq('metric_key', metric);

    const { data: facts, error: fErr } = await q;
    if (fErr) return res.status(500).json({ error: fErr.message });

    // 3) Narratifs
    let nq = supabase
      .from('fact_narratives')
      .select('*')
      .eq('company_slug', comp.slug)
      .order('as_of_date', { ascending: false })
      .limit(limit);

    if (metric) nq = nq.eq('metric_key', metric);

    const { data: narr, error: nErr } = await nq;
    if (nErr) return res.status(500).json({ error: nErr.message });

    // 4) Regroupement produit
    const metrics = {};
    for (const row of facts || []) {
      if (!metrics[row.metric_key]) metrics[row.metric_key] = { series: [], last: null };
      metrics[row.metric_key].series.push({
        date: row.as_of_date,
        value: row.metric_value_num,
        yoy: row.yoy_change,
        qoq: row.qoq_change,
        trend: row.trend,
        signal: row.signal_strength,
        zscore_sector: row.zscore_sector
      });
    }
    // Dernier point par metric
    for (const k of Object.keys(metrics)) {
      metrics[k].series.sort((a,b) => new Date(b.date) - new Date(a.date));
      metrics[k].last = metrics[k].series[0];
    }

    // 5) Narratifs simplifiés
    const narratives = (narr || []).map(n => ({
      date: n.as_of_date,
      metric: n.metric_key,
      fr: n.narrative_fr,
      en: n.narrative_en,
      yoy: n.yoy_change,
      qoq: n.qoq_change,
      trend: n.trend,
      signal: n.signal_strength
    }));

    return res.status(200).json({
      company: { slug: comp.slug, name: comp.name, domain: comp.domain },
      metrics,
      narratives
    });
  } catch (err) {
    console.error('READ ERROR:', err);
    return res.status(500).json({ error: err?.message || 'unknown' });
  }
};
