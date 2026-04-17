import { useState, useMemo } from "react";

const PALETTE = {
  purple: ["#EEEDFE","#CECBF6","#AFA9EC","#7F77DD","#534AB7","#3C3489","#26215C"],
  teal:   ["#E1F5EE","#9FE1CB","#5DCAA5","#1D9E75","#0F6E56","#085041","#04342C"],
  coral:  ["#FAECE7","#F5C4B3","#F0997B","#D85A30","#993C1D","#712B13","#4A1B0C"],
  amber:  ["#FAEEDA","#FAC775","#EF9F27","#BA7517","#854F0B","#633806","#412402"],
  blue:   ["#E6F1FB","#B5D4F4","#85B7EB","#378ADD","#185FA5","#0C447C","#042C53"],
  green:  ["#EAF3DE","#C0DD97","#97C459","#639922","#3B6D11","#27500A","#173404"],
};

// ── Mock data mirroring the star schema ───────────────────────────────────────
const TOP_SKILLS = [
  { skill_name:"python",         job_count:142, pct:18.2 },
  { skill_name:"sql",            job_count:129, pct:16.5 },
  { skill_name:"docker",         job_count:98,  pct:12.5 },
  { skill_name:"aws",            job_count:87,  pct:11.1 },
  { skill_name:"git",            job_count:82,  pct:10.5 },
  { skill_name:"postgresql",     job_count:74,  pct:9.5  },
  { skill_name:"apache spark",   job_count:61,  pct:7.8  },
  { skill_name:"tensorflow",     job_count:54,  pct:6.9  },
  { skill_name:"airflow",        job_count:48,  pct:6.1  },
  { skill_name:"power bi",       job_count:43,  pct:5.5  },
  { skill_name:"tableau",        job_count:38,  pct:4.9  },
  { skill_name:"kafka",          job_count:36,  pct:4.6  },
  { skill_name:"dbt",            job_count:29,  pct:3.7  },
  { skill_name:"scikit-learn",   job_count:27,  pct:3.5  },
  { skill_name:"node.js",        job_count:24,  pct:3.1  },
];

const JOBS_BY_CITY = [
  { city:"Cairo",      job_count:267 },
  { city:"Giza",       job_count:84  },
  { city:"Remote",     job_count:61  },
  { city:"Alexandria", job_count:33  },
  { city:"Unknown",    job_count:17  },
];

const JOBS_BY_EXPERIENCE = [
  { level:"entry",   label:"0-1 years",   min_years:0,  job_count:58  },
  { level:"entry",   label:"1-2 years",   min_years:1,  job_count:74  },
  { level:"mid",     label:"2-4 years",   min_years:2,  job_count:112 },
  { level:"mid",     label:"3-5 years",   min_years:3,  job_count:89  },
  { level:"senior",  label:"5-7 years",   min_years:5,  job_count:63  },
  { level:"senior",  label:"5-10 years",  min_years:5,  job_count:41  },
  { level:"lead",    label:"8+ years",    min_years:8,  job_count:22  },
  { level:"unknown", label:"Not specified",min_years:null,job_count:3 },
];

const TOP_COMPANIES = [
  { company_name:"Vodafone Egypt",        job_count:28 },
  { company_name:"Fawry",                 job_count:23 },
  { company_name:"IBM Egypt",             job_count:21 },
  { company_name:"Instabug",              job_count:18 },
  { company_name:"Paymob",               job_count:17 },
  { company_name:"Microsoft Egypt",       job_count:15 },
  { company_name:"Robusta Studio",        job_count:14 },
  { company_name:"ITWorx",                job_count:13 },
  { company_name:"Raya Holding",          job_count:12 },
  { company_name:"Link Development",      job_count:11 },
];

const DAILY_TREND = [
  { full_date:"2026-04-01", jobs_scraped:38 },
  { full_date:"2026-04-02", jobs_scraped:42 },
  { full_date:"2026-04-03", jobs_scraped:45 },
  { full_date:"2026-04-04", jobs_scraped:39 },
  { full_date:"2026-04-05", jobs_scraped:51 },
  { full_date:"2026-04-06", jobs_scraped:47 },
  { full_date:"2026-04-07", jobs_scraped:33 },
  { full_date:"2026-04-08", jobs_scraped:44 },
  { full_date:"2026-04-09", jobs_scraped:56 },
  { full_date:"2026-04-10", jobs_scraped:49 },
  { full_date:"2026-04-11", jobs_scraped:52 },
  { full_date:"2026-04-12", jobs_scraped:48 },
  { full_date:"2026-04-13", jobs_scraped:61 },
  { full_date:"2026-04-14", jobs_scraped:57 },
  { full_date:"2026-04-15", jobs_scraped:43 },
  { full_date:"2026-04-16", jobs_scraped:59 },
  { full_date:"2026-04-17", jobs_scraped:38 },
];

const JOBS_BY_KEYWORD = [
  { keyword:"data engineer",    job_count:128 },
  { keyword:"data analyst",     job_count:116 },
  { keyword:"machine learning", job_count:97  },
  { keyword:"backend developer",job_count:121 },
];

const LEVEL_COLORS = {
  entry:   { bg: PALETTE.teal[0],   border: PALETTE.teal[2],   text: PALETTE.teal[5]   },
  mid:     { bg: PALETTE.blue[0],   border: PALETTE.blue[2],   text: PALETTE.blue[5]   },
  senior:  { bg: PALETTE.purple[0], border: PALETTE.purple[2], text: PALETTE.purple[5] },
  lead:    { bg: PALETTE.coral[0],  border: PALETTE.coral[2],  text: PALETTE.coral[5]  },
  unknown: { bg: "#F1EFE8",         border: "#B4B2A9",         text: "#5F5E5A"         },
};

// ── Tiny chart primitives ─────────────────────────────────────────────────────
function HBar({ value, max, color }) {
  const pct = Math.round((value / max) * 100);
  return (
    <div style={{ display:"flex", alignItems:"center", gap:8 }}>
      <div style={{ flex:1, height:8, background:"var(--color-background-secondary)", borderRadius:4, overflow:"hidden" }}>
        <div style={{ width:`${pct}%`, height:"100%", background:color, borderRadius:4, transition:"width .4s ease" }} />
      </div>
    </div>
  );
}

function Sparkline({ data, color }) {
  const vals = data.map(d => d.jobs_scraped);
  const mn = Math.min(...vals), mx = Math.max(...vals);
  const W = 200, H = 50, pad = 4;
  const pts = vals.map((v, i) => {
    const x = pad + (i / (vals.length - 1)) * (W - 2 * pad);
    const y = pad + (1 - (v - mn) / (mx - mn || 1)) * (H - 2 * pad);
    return `${x},${y}`;
  }).join(" ");
  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width:"100%", height:H }}>
      <polyline points={pts} fill="none" stroke={color} strokeWidth="2" strokeLinejoin="round" />
      <circle cx={pts.split(" ").at(-1).split(",")[0]} cy={pts.split(" ").at(-1).split(",")[1]} r="3" fill={color} />
    </svg>
  );
}

function DonutChart({ data, colors }) {
  const total = data.reduce((s, d) => s + d.job_count, 0);
  const R = 56, cx = 70, cy = 70, stroke = 20;
  let cumAngle = -Math.PI / 2;
  const slices = data.map((d, i) => {
    const angle = (d.job_count / total) * 2 * Math.PI;
    const x1 = cx + R * Math.cos(cumAngle);
    const y1 = cy + R * Math.sin(cumAngle);
    cumAngle += angle;
    const x2 = cx + R * Math.cos(cumAngle);
    const y2 = cy + R * Math.sin(cumAngle);
    const large = angle > Math.PI ? 1 : 0;
    return { d: `M${x1},${y1} A${R},${R} 0 ${large} 1 ${x2},${y2}`, color: colors[i % colors.length], count: d.job_count, label: d.keyword || d.city };
  });
  return (
    <svg viewBox="0 0 140 140" style={{ width:140, height:140 }}>
      {slices.map((s, i) => (
        <path key={i} d={s.d} fill="none" stroke={s.color} strokeWidth={stroke} />
      ))}
      <text x={cx} y={cy - 6} textAnchor="middle" style={{ fontSize:18, fontWeight:500, fill:"var(--color-text-primary)" }}>{total}</text>
      <text x={cx} y={cy + 12} textAnchor="middle" style={{ fontSize:10, fill:"var(--color-text-secondary)" }}>total</text>
    </svg>
  );
}

// ── Metric card ───────────────────────────────────────────────────────────────
function MetricCard({ label, value, sub, accent }) {
  return (
    <div style={{ background:"var(--color-background-secondary)", borderRadius:8, padding:"14px 16px", minWidth:0 }}>
      <div style={{ fontSize:12, color:"var(--color-text-secondary)", marginBottom:4 }}>{label}</div>
      <div style={{ fontSize:26, fontWeight:500, color: accent || "var(--color-text-primary)", lineHeight:1 }}>{value}</div>
      {sub && <div style={{ fontSize:11, color:"var(--color-text-secondary)", marginTop:4 }}>{sub}</div>}
    </div>
  );
}

// ── Section header ────────────────────────────────────────────────────────────
function SectionHeader({ title }) {
  return (
    <div style={{ fontSize:13, fontWeight:500, color:"var(--color-text-secondary)", textTransform:"uppercase", letterSpacing:"0.08em", marginBottom:12, marginTop:4 }}>
      {title}
    </div>
  );
}

// ── Main dashboard ────────────────────────────────────────────────────────────
export default function Dashboard() {
  const [skillsLimit, setSkillsLimit] = useState(10);
  const [activeTab, setActiveTab] = useState("overview");

  const totalJobs = JOBS_BY_KEYWORD.reduce((s, d) => s + d.job_count, 0);
  const totalCompanies = TOP_COMPANIES.length;
  const avgSkillsPerJob = 6.4;
  const latestDate = DAILY_TREND.at(-1).full_date;

  const keywordColors = [PALETTE.purple[3], PALETTE.teal[3], PALETTE.coral[3], PALETTE.blue[3]];
  const cityColors = [PALETTE.blue[3], PALETTE.blue[2], PALETTE.teal[3], PALETTE.amber[3], PALETTE.purple[3]];

  const skillsMax = TOP_SKILLS[0].job_count;
  const cityMax   = JOBS_BY_CITY[0].job_count;
  const compMax   = TOP_COMPANIES[0].job_count;
  const expMax    = Math.max(...JOBS_BY_EXPERIENCE.map(d => d.job_count));

  const tabs = ["overview", "skills", "companies", "trends"];

  return (
    <div style={{ padding:"1.5rem 0", fontFamily:"var(--font-sans)", color:"var(--color-text-primary)", maxWidth:880, margin:"0 auto" }}>
      <h2 style={{ visibility:"hidden", position:"absolute" }}>Egyptian job market analytics dashboard</h2>

      {/* Header */}
      <div style={{ marginBottom:24 }}>
        <div style={{ display:"flex", alignItems:"baseline", justifyContent:"space-between", flexWrap:"wrap", gap:8 }}>
          <div>
            <div style={{ fontSize:11, color:"var(--color-text-secondary)", fontWeight:500, letterSpacing:"0.08em", textTransform:"uppercase", marginBottom:4 }}>Egyptian job market analytics</div>
            <h1 style={{ fontSize:22, fontWeight:500, margin:0 }}>Pipeline dashboard</h1>
          </div>
          <div style={{ fontSize:12, color:"var(--color-text-secondary)", background:"var(--color-background-secondary)", padding:"4px 10px", borderRadius:6, border:"0.5px solid var(--color-border-tertiary)" }}>
            Last run: {latestDate}
          </div>
        </div>
      </div>

      {/* KPI strip */}
      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(140px, 1fr))", gap:10, marginBottom:28 }}>
        <MetricCard label="Total job postings" value={totalJobs.toLocaleString()} sub="across 4 keywords" accent={PALETTE.purple[4]} />
        <MetricCard label="Unique companies" value={totalCompanies} sub="actively hiring" accent={PALETTE.teal[4]} />
        <MetricCard label="Avg skills / job" value={avgSkillsPerJob.toFixed(1)} sub="after normalisation" accent={PALETTE.blue[4]} />
        <MetricCard label="Days tracked" value={DAILY_TREND.length} sub="daily scrape runs" accent={PALETTE.coral[3]} />
      </div>

      {/* Tabs */}
      <div style={{ display:"flex", gap:4, marginBottom:24, borderBottom:"0.5px solid var(--color-border-tertiary)", paddingBottom:0 }}>
        {tabs.map(t => (
          <button key={t} onClick={() => setActiveTab(t)}
            style={{ padding:"6px 14px", fontSize:13, fontWeight: t === activeTab ? 500 : 400,
              color: t === activeTab ? "var(--color-text-primary)" : "var(--color-text-secondary)",
              background:"transparent", border:"none", borderBottom: t === activeTab ? `2px solid ${PALETTE.purple[4]}` : "2px solid transparent",
              cursor:"pointer", textTransform:"capitalize", marginBottom:-1 }}>
            {t}
          </button>
        ))}
      </div>

      {/* ── OVERVIEW TAB ─────────────────────────────────────────────────── */}
      {activeTab === "overview" && (
        <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:20 }}>

          {/* Jobs by keyword */}
          <div style={{ background:"var(--color-background-primary)", border:"0.5px solid var(--color-border-tertiary)", borderRadius:12, padding:"16px 18px" }}>
            <SectionHeader title="Jobs by keyword" />
            <div style={{ display:"flex", alignItems:"center", gap:16 }}>
              <DonutChart data={JOBS_BY_KEYWORD} colors={keywordColors} />
              <div style={{ flex:1, display:"flex", flexDirection:"column", gap:8 }}>
                {JOBS_BY_KEYWORD.map((d, i) => (
                  <div key={i}>
                    <div style={{ display:"flex", justifyContent:"space-between", fontSize:12, marginBottom:3 }}>
                      <span style={{ color:"var(--color-text-primary)" }}>{d.keyword}</span>
                      <span style={{ color:"var(--color-text-secondary)", fontWeight:500 }}>{d.job_count}</span>
                    </div>
                    <HBar value={d.job_count} max={totalJobs} color={keywordColors[i]} />
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Jobs by city */}
          <div style={{ background:"var(--color-background-primary)", border:"0.5px solid var(--color-border-tertiary)", borderRadius:12, padding:"16px 18px" }}>
            <SectionHeader title="Jobs by city" />
            <div style={{ display:"flex", alignItems:"center", gap:16 }}>
              <DonutChart data={JOBS_BY_CITY} colors={cityColors} />
              <div style={{ flex:1, display:"flex", flexDirection:"column", gap:8 }}>
                {JOBS_BY_CITY.map((d, i) => (
                  <div key={i}>
                    <div style={{ display:"flex", justifyContent:"space-between", fontSize:12, marginBottom:3 }}>
                      <span style={{ color:"var(--color-text-primary)" }}>{d.city}</span>
                      <span style={{ color:"var(--color-text-secondary)", fontWeight:500 }}>{d.job_count}</span>
                    </div>
                    <HBar value={d.job_count} max={cityMax} color={cityColors[i]} />
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Experience breakdown */}
          <div style={{ background:"var(--color-background-primary)", border:"0.5px solid var(--color-border-tertiary)", borderRadius:12, padding:"16px 18px", gridColumn:"1 / -1" }}>
            <SectionHeader title="Jobs by experience level" />
            <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fill, minmax(180px, 1fr))", gap:10 }}>
              {JOBS_BY_EXPERIENCE.filter(d => d.level !== "unknown").map((d, i) => {
                const lc = LEVEL_COLORS[d.level];
                return (
                  <div key={i} style={{ background:lc.bg, border:`0.5px solid ${lc.border}`, borderRadius:8, padding:"10px 12px" }}>
                    <div style={{ fontSize:11, fontWeight:500, color:lc.text, textTransform:"uppercase", letterSpacing:"0.06em", marginBottom:4 }}>{d.level}</div>
                    <div style={{ fontSize:20, fontWeight:500, color:lc.text, marginBottom:2 }}>{d.job_count}</div>
                    <div style={{ fontSize:11, color:lc.text, opacity:0.75 }}>{d.label}</div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}

      {/* ── SKILLS TAB ───────────────────────────────────────────────────── */}
      {activeTab === "skills" && (
        <div style={{ background:"var(--color-background-primary)", border:"0.5px solid var(--color-border-tertiary)", borderRadius:12, padding:"16px 18px" }}>
          <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", marginBottom:16 }}>
            <SectionHeader title="Top skills in demand" />
            <div style={{ display:"flex", gap:6 }}>
              {[5, 10, 15].map(n => (
                <button key={n} onClick={() => setSkillsLimit(n)}
                  style={{ padding:"3px 10px", fontSize:12, borderRadius:6,
                    background: skillsLimit === n ? PALETTE.purple[4] : "transparent",
                    color: skillsLimit === n ? "#fff" : "var(--color-text-secondary)",
                    border:`0.5px solid ${skillsLimit === n ? PALETTE.purple[4] : "var(--color-border-tertiary)"}`,
                    cursor:"pointer" }}>
                  Top {n}
                </button>
              ))}
            </div>
          </div>
          <div style={{ display:"flex", flexDirection:"column", gap:10 }}>
            {TOP_SKILLS.slice(0, skillsLimit).map((s, i) => (
              <div key={i}>
                <div style={{ display:"flex", justifyContent:"space-between", alignItems:"baseline", fontSize:13, marginBottom:5 }}>
                  <div style={{ display:"flex", alignItems:"center", gap:8 }}>
                    <span style={{ fontSize:11, color:"var(--color-text-secondary)", minWidth:18, textAlign:"right" }}>{i + 1}</span>
                    <span style={{ fontWeight:500 }}>{s.skill_name}</span>
                  </div>
                  <div style={{ display:"flex", gap:12, color:"var(--color-text-secondary)", fontSize:12 }}>
                    <span>{s.job_count} jobs</span>
                    <span style={{ color: PALETTE.purple[4], fontWeight:500, minWidth:36, textAlign:"right" }}>{s.pct}%</span>
                  </div>
                </div>
                <div style={{ height:8, background:"var(--color-background-secondary)", borderRadius:4, overflow:"hidden" }}>
                  <div style={{ width:`${(s.job_count / skillsMax) * 100}%`, height:"100%",
                    background: i < 3 ? PALETTE.purple[3] : i < 7 ? PALETTE.purple[2] : PALETTE.purple[1],
                    borderRadius:4, transition:"width .4s ease" }} />
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── COMPANIES TAB ────────────────────────────────────────────────── */}
      {activeTab === "companies" && (
        <div style={{ background:"var(--color-background-primary)", border:"0.5px solid var(--color-border-tertiary)", borderRadius:12, padding:"16px 18px" }}>
          <SectionHeader title="Top hiring companies" />
          <div style={{ display:"flex", flexDirection:"column", gap:10 }}>
            {TOP_COMPANIES.map((c, i) => {
              const initials = c.company_name.split(" ").slice(0, 2).map(w => w[0]).join("").toUpperCase();
              const avatarBg = [PALETTE.teal[1], PALETTE.purple[1], PALETTE.blue[1], PALETTE.coral[1], PALETTE.amber[1]];
              const avatarTx = [PALETTE.teal[5], PALETTE.purple[5], PALETTE.blue[5], PALETTE.coral[5], PALETTE.amber[5]];
              return (
                <div key={i} style={{ display:"flex", alignItems:"center", gap:12, padding:"10px 12px", background:"var(--color-background-secondary)", borderRadius:8 }}>
                  <div style={{ width:36, height:36, borderRadius:"50%", background:avatarBg[i % 5], display:"flex", alignItems:"center", justifyContent:"center",
                    fontSize:12, fontWeight:500, color:avatarTx[i % 5], flexShrink:0 }}>{initials}</div>
                  <div style={{ flex:1, minWidth:0 }}>
                    <div style={{ fontSize:13, fontWeight:500, marginBottom:3 }}>{c.company_name}</div>
                    <div style={{ height:6, background:"var(--color-border-tertiary)", borderRadius:3, overflow:"hidden" }}>
                      <div style={{ width:`${(c.job_count / compMax) * 100}%`, height:"100%", background:PALETTE.teal[3], borderRadius:3 }} />
                    </div>
                  </div>
                  <div style={{ fontSize:14, fontWeight:500, color:PALETTE.teal[4], minWidth:28, textAlign:"right" }}>{c.job_count}</div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* ── TRENDS TAB ───────────────────────────────────────────────────── */}
      {activeTab === "trends" && (
        <div style={{ display:"flex", flexDirection:"column", gap:20 }}>
          {/* Sparkline trend */}
          <div style={{ background:"var(--color-background-primary)", border:"0.5px solid var(--color-border-tertiary)", borderRadius:12, padding:"16px 18px" }}>
            <SectionHeader title="Daily scrape trend" />
            <div style={{ display:"flex", gap:20, marginBottom:12 }}>
              <MetricCard label="Total scraped" value={DAILY_TREND.reduce((s,d)=>s+d.jobs_scraped,0)} sub="this period" accent={PALETTE.purple[4]} />
              <MetricCard label="Peak day" value={Math.max(...DAILY_TREND.map(d=>d.jobs_scraped))} sub={DAILY_TREND.find(d=>d.jobs_scraped===Math.max(...DAILY_TREND.map(d=>d.jobs_scraped))).full_date} accent={PALETTE.teal[4]} />
              <MetricCard label="Avg / day" value={Math.round(DAILY_TREND.reduce((s,d)=>s+d.jobs_scraped,0)/DAILY_TREND.length)} sub="jobs scraped" accent={PALETTE.blue[4]} />
            </div>
            <div style={{ position:"relative" }}>
              {/* Simple SVG bar chart */}
              <svg viewBox="0 0 820 120" style={{ width:"100%", height:120 }}>
                {DAILY_TREND.map((d, i) => {
                  const mn = Math.min(...DAILY_TREND.map(x=>x.jobs_scraped));
                  const mx = Math.max(...DAILY_TREND.map(x=>x.jobs_scraped));
                  const barW = 820 / DAILY_TREND.length - 4;
                  const x = i * (820 / DAILY_TREND.length) + 2;
                  const h = Math.round(((d.jobs_scraped - mn) / (mx - mn || 1)) * 80 + 12);
                  const isMax = d.jobs_scraped === mx;
                  return (
                    <g key={i}>
                      <rect x={x} y={120 - h} width={barW} height={h} rx="2"
                        fill={isMax ? PALETTE.purple[4] : PALETTE.purple[1]} />
                      {i % 4 === 0 && (
                        <text x={x + barW / 2} y={118} textAnchor="middle"
                          style={{ fontSize:8, fill:"var(--color-text-secondary)" }}>
                          {d.full_date.slice(5)}
                        </text>
                      )}
                    </g>
                  );
                })}
              </svg>
            </div>
          </div>

          {/* Pipeline run quality */}
          <div style={{ background:"var(--color-background-primary)", border:"0.5px solid var(--color-border-tertiary)", borderRadius:12, padding:"16px 18px" }}>
            <SectionHeader title="Pipeline stage summary" />
            <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(160px, 1fr))", gap:10 }}>
              {[
                { stage:"scrape",   count:totalJobs, label:"Raw jobs scraped",       color:PALETTE.teal  },
                { stage:"validate", count:totalJobs, label:"Passed validation",      color:PALETTE.green },
                { stage:"kafka",    count:totalJobs, label:"Published to Kafka",     color:PALETTE.blue  },
                { stage:"staging",  count:totalJobs, label:"Written to staging",     color:PALETTE.purple},
                { stage:"warehouse",count:Math.round(totalJobs * 0.97), label:"Loaded to warehouse", color:PALETTE.teal  },
              ].map((s, i) => (
                <div key={i} style={{ background:s.color[0], border:`0.5px solid ${s.color[2]}`, borderRadius:8, padding:"12px 14px" }}>
                  <div style={{ fontSize:11, color:s.color[5], fontWeight:500, marginBottom:4 }}>{s.stage}</div>
                  <div style={{ fontSize:20, fontWeight:500, color:s.color[5] }}>{s.count.toLocaleString()}</div>
                  <div style={{ fontSize:11, color:s.color[4], marginTop:2 }}>{s.label}</div>
                </div>
              ))}
            </div>
          </div>

          {/* Incremental staging info */}
          <div style={{ background:PALETTE.purple[0], border:`0.5px solid ${PALETTE.purple[2]}`, borderRadius:12, padding:"14px 18px", display:"flex", alignItems:"flex-start", gap:12 }}>
            <div style={{ fontSize:16, color:PALETTE.purple[4], marginTop:1 }}>✦</div>
            <div>
              <div style={{ fontSize:13, fontWeight:500, color:PALETTE.purple[5], marginBottom:4 }}>Incremental staging active</div>
              <div style={{ fontSize:12, color:PALETTE.purple[4], lineHeight:1.6 }}>
                Each DAG run upserts into fixed per-keyword staging files (<code style={{ fontSize:11 }}>staging_&lt;keyword&gt;.json</code>).
                New job IDs are appended; existing records are refreshed in-place.
                No staging bloat — the dataset grows cleanly over time.
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Footer */}
      <div style={{ marginTop:28, paddingTop:16, borderTop:"0.5px solid var(--color-border-tertiary)", fontSize:11, color:"var(--color-text-secondary)", display:"flex", justifyContent:"space-between", flexWrap:"wrap", gap:8 }}>
        <span>Egyptian Job Market Analytics Pipeline · Phase 5</span>
        <span>Source: Wuzzuf.net · Warehouse: PostgreSQL star schema</span>
      </div>
    </div>
  );
}
