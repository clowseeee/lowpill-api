// /api/read.js — Lowpill v1.7 (read + provenance-aware + metrics clean)
const { createClient } = require('@supabase/supabase-js');
const { z } = require('zod');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE);

// -------- utils ----------
const toSlug = (s) => (s || '').trim().toLowerCase();
const isFiniteNum = (x) => Number.isFinite(x) && !Number.isNaN(x);

// -------- validation ----------
const querySchema = z.object({
  company: z.string().min(1),
  metric: z.string().optional(),
  theme: z.string().optional(),
  limit: z.coerce.number().int().min(1).max(50).optional()
});

// -------- handler ----------
module.exports = async (req, res) => {
  try {
    if (req.method !== 'GET') {
      return res.status(405).json({ error: 'Method not allowed' });
    }

    // 1️⃣ Parse & validate
    const parsed = querySchema.parse({
      company: req.query.company,
      metric: req.query.metric,
      theme: req.query.theme,
      limit: req.query.limit
    });

    const slug = toSlug(parsed.company);

    // 2️⃣ Company lookup
    const { data: company, error: cErr } = await supabase
      .from('companies')
      .select('id, slug, name, domain')
      .eq('slug', slug)
      .maybeSingle();
    if (cErr) return res.status(500).json({ error: `company select: ${cErr.message}` });
    if (!company) return res.status(404).json({ error: 'company not found' });

    // 3️⃣ Metrics (cleaned, deduped, scored)
    let metrics = {};
    if (parsed.metric) {
      const { data: factRows, error: fErr } = await supabase
        .from('facts')
        .select('as_of_date, metric_value_num')
        .eq('company_id', company.id)
        .eq('metric_key', parsed.metric)
        .not('as_of_date', 'is', null)
        .distinct(['as_of_date', 'metric_value_num'])
        .order('as_of_date', { ascending: true });

      if (fErr) return res.status(500).json({ error: `facts select: ${fErr.message}` });

      // 🔹 déduplication des dates
      const dedupMap = new Map();
      for (const r of (factRows || [])) {
        if (!r.as_of_date || r.metric_value_num == null) continue;
        dedupMap.set(r.as_of_date, Number(r.metric_value_num));
      }

      const series = [...dedupMap.entries()]
        .sort((a, b) => new Date(a[0]) - new Date(b[0]))
        .map(([date, value]) => ({ date, value }));

      // 🔹 calcule variations + signaux
      for (let i = 0; i < series.length; i++) {
        const cur = series[i];
        const prev = i > 0 ? series[i - 1] : null;
        const qoq = prev && Number.isFinite(prev.value) && prev.value !== 0
          ? Number(((100 * (cur.value - prev.value)) / prev.value).toFixed(2))
          : null;
        const yoy = qoq;

        let signal = 'none';
        const abs = qoq != null ? Math.abs(qoq) : null;
        if (abs != null) {
          if (abs > 10) signal = 'strong';
          else if (abs >= 5) signal = 'moderate';
          else if (abs >= 2) signal = 'weak';
        }
        const trend = qoq == null ? 'flat' : (qoq > 0 ? 'up' : (qoq < 0 ? 'down' : 'flat'));

        series[i] = {
          date: cur.date,
          value: cur.value,
          yoy, qoq, trend, signal,
          zscore_sector: null // futur lien avec ta vue sectorielle
        };
      }

      const last = series.length ? series[series.length - 1] : null;
      metrics[parsed.metric] = {
        series,
        last: last ? { ...last } : null
      };
    }

    // 4️⃣ Insights + provenance
    const lim = parsed.limit ?? 5;
    const themeFilter = parsed.theme ? { theme_enum: toSlug(parsed.theme) } : {};

    const { data: insights, error: iErr } = await supabase
      .from('insights')
      .select(`
        id, company_id, source_id, theme_enum, theme, text, confidence, created_at,
        sources:source_id (
          id, url, title, published_at,
          publisher_domain, publisher_name, publisher_type, is_official, trust_score, source_type
        )
      `)
      .eq('company_id', company.id)
      .match(themeFilter)
      .order('created_at', { ascending: false });

    if (iErr) return res.status(500).json({ error: `insights select: ${iErr.message}` });

    const enriched = (insights || []).map(r => {
      const trust = r?.sources?.trust_score ?? 0;
      const conf = r?.confidence ?? 0;
      const provenance_score = Number((conf * trust).toFixed(3));
      return {
        id: r.id,
        date: r.created_at,
        theme: r.theme ?? r.theme_enum ?? 'other',
        text: r.text,
        confidence: conf,
        provenance_score,
        publisher: {
          name: r?.sources?.publisher_name ?? null,
          domain: r?.sources?.publisher_domain ?? null,
          type: r?.sources?.publisher_type ?? 'other',
          is_official: !!r?.sources?.is_official,
          trust_score: r?.sources?.trust_score ?? 0,
          doc_type: r?.sources?.source_type ?? 'other',
          url: r?.sources?.url ?? null,
          title: r?.sources?.title ?? null,
          published_at: r?.sources?.published_at ?? null
        }
      };
    });

    enriched.sort((a, b) => (b.provenance_score - a.provenance_score) || (new Date(b.date) - new Date(a.date)));

    const topInsights = enriched.slice(0, lim);
    const avgProv = topInsights.length
      ? Number((topInsights.reduce((s, x) => s + (x.provenance_score ?? 0), 0) / topInsights.length).toFixed(3))
      : null;

    const byType = topInsights.reduce((acc, x) => {
      const k = x.publisher?.type || 'other';
      acc[k] = (acc[k] || 0) + 1;
      return acc;
    }, {});

    // 5️⃣ Narratives simples (corrigées)
    const narratives = [];
    if (parsed.metric && metrics[parsed.metric]?.series?.length) {
      for (const s of metrics[parsed.metric].series) {
        if (!s.date) continue;
        const yr = new Date(s.date).getFullYear();
        narratives.push({
          date: s.date,
          metric: parsed.metric,
          fr: s.yoy != null
            ? `${company.name} affiche ${s.yoy > 0 ? 'une hausse' : 'une baisse'} de ${Math.abs(s.yoy).toFixed(2)}% de son ${parsed.metric} en ${yr}`
            : `${company.name} n'a pas d'historique comparable pour ${parsed.metric} en ${yr}`,
          en: s.yoy != null
            ? `${company.name}’s ${parsed.metric} ${s.yoy > 0 ? 'rose' : 'fell'} ${Math.abs(s.yoy).toFixed(2)}% YoY`
            : `${company.name} has no prior period to compare for ${parsed.metric} in ${yr}`,
          yoy: s.yoy,
          qoq: s.qoq,
          trend: s.trend,
          signal: s.signal
        });
      }
    }

    // 6️⃣ Response
    return res.status(200).json({
      company: { slug: company.slug, name: company.name, domain: company.domain ?? null },
      metrics,
      insights: {
        top: topInsights,
        aggregates: {
          avg_provenance_score: avgProv,
          count_by_publisher_type: byType
        }
      },
      narratives
    });

  } catch (err) {
    console.error('READ ERROR:', err);
    return res.status(500).json({ error: err?.message || 'unknown' });
  }
};
