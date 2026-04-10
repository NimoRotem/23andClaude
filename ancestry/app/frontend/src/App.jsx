import { useState, useEffect, useRef, useCallback } from "react";

/* ═══════════════════════════════════════════════════════════════
   API + Helpers
   ═══════════════════════════════════════════════════════════════ */
const API = "/ancestry/api";
async function api(path, opts) {
  const res = await fetch(`${API}${path}`, opts);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail || `HTTP ${res.status}`);
  }
  return res.json();
}

const GROUP_COLORS = {
  European: "#58a6ff", Finnish: "#79c0ff", EastAsian: "#56d364",
  SoutheastAsian: "#39d353", African: "#d29922", American: "#f778ba",
  SouthAsian: "#bc8cff", MiddleEastern: "#ff7b72", Oceanian: "#3fb950",
  AshkenaziJewish: "#e3b341",
};
function groupColor(g) { return GROUP_COLORS[g] || "#8b949e"; }

/** Format primary_pct — backend may return 0-1 (old) or 0-100 (new). Always display as %. */
function fmtPct(v) {
  if (typeof v !== "number") return "?";
  return (v <= 1 ? v * 100 : v).toFixed(1);
}

/** Ancestry group descriptions for context */
const GROUP_INFO = {
  European: { emoji: "🏰", region: "Europe", desc: "Genetic ancestry tracing to European populations including Western, Southern, Eastern, and Northern Europe. Common reference populations: French, Italian, British, Spanish, Sardinian, Russian." },
  Finnish: { emoji: "🌲", region: "Finland", desc: "Finnish populations have a distinct genetic profile due to a historical population bottleneck. Genetically related to other Europeans but with unique founder effects and elevated runs of homozygosity." },
  EastAsian: { emoji: "🏯", region: "East Asia", desc: "Genetic ancestry from East Asian populations including Han Chinese, Japanese, and Korean groups. One of the most genetically distinct continental clusters." },
  SoutheastAsian: { emoji: "🌴", region: "Southeast Asia", desc: "Genetic ancestry from Southeast Asian populations including Kinh Vietnamese, Cambodian, Dai, and Lahu. Shows a gradient between East Asian and Oceanian clusters." },
  African: { emoji: "🌍", region: "Sub-Saharan Africa", desc: "The most genetically diverse continental group, reflecting humanity's deepest roots. Includes West African (Yoruba, Mandinka), East African (Luhya), and Southern African (San, Bantu) populations." },
  American: { emoji: "🌎", region: "Americas (Indigenous)", desc: "Indigenous American ancestry from populations with deep roots in the Americas. Reference populations include Maya, Pima, Karitiana, and Surui. Distinct from post-Columbian admixed populations." },
  SouthAsian: { emoji: "🕌", region: "South & Central Asia", desc: "Genetic ancestry from the Indian subcontinent and Central Asia, including Punjabi, Bengali, Gujarati, Balochi, Pathan, and Kalash populations. Shows a gradient from West to East." },
  MiddleEastern: { emoji: "🏺", region: "Middle East & North Africa", desc: "Levantine and North African ancestry including Druze, Palestinian, Bedouin, and Mozabite populations. Critical for detecting Ashkenazi Jewish ancestry, which shows a characteristic European + Middle Eastern mix." },
  Oceanian: { emoji: "🏝️", region: "Oceania & Melanesia", desc: "Ancestry from Papua New Guinea, Melanesian islands, and Bougainville. Among the most genetically isolated populations, carrying ancient Denisovan admixture at higher levels than other modern humans." },
  AshkenaziJewish: { emoji: "✡️", region: "Ashkenazi Diaspora", desc: "Ashkenazi Jewish ancestry reflects a founder population with roots in the Levant and medieval Europe. Characterized by a distinctive European + Middle Eastern admixture pattern and elevated runs of homozygosity due to endogamy." },
};

/** Approximate geographic center for each group (lon, lat) for the world map */
const GROUP_GEO = {
  European: [15, 50], Finnish: [26, 64], EastAsian: [110, 35],
  SoutheastAsian: [105, 15], African: [20, 0], American: [-80, 10],
  SouthAsian: [75, 25], MiddleEastern: [38, 32], Oceanian: [147, -6],
  AshkenaziJewish: [20, 48],
};

function timeAgo(iso) {
  if (!iso) return "";
  const sec = Math.floor((Date.now() - new Date(iso)) / 1000);
  if (sec < 60) return `${sec}s ago`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
  if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
  return `${Math.floor(sec / 86400)}d ago`;
}

/* pop2group mapping for population-level results */
const POP_TO_GROUP = {
  CEU:"European",TSI:"European",GBR:"European",IBS:"European",French:"European",
  Sardinian:"European",Tuscan:"European",Basque:"European",BergamoItalian:"European",
  Orcadian:"European",Russian:"European",Adygei:"European",Italian:"European",
  FIN:"Finnish",
  CHB:"EastAsian",JPT:"EastAsian",CHS:"EastAsian",CDX:"EastAsian",KHV:"EastAsian",
  Han:"EastAsian",NorthernHan:"EastAsian",Japanese:"EastAsian",Dai:"EastAsian",
  She:"EastAsian",Tujia:"EastAsian",Miao:"EastAsian",Naxi:"EastAsian",Yi:"EastAsian",
  Tu:"EastAsian",Xibo:"EastAsian",Mongola:"EastAsian",Hezhen:"EastAsian",
  Daur:"EastAsian",Oroqen:"EastAsian",Cambodian:"EastAsian",Lahu:"EastAsian",Yakut:"EastAsian",
  YRI:"African",LWK:"African",GWD:"African",MSL:"African",ESN:"African",ACB:"African",
  ASW:"African",Yoruba:"African",Mandenka:"African",BantuSouthAfrica:"African",
  BantuKenya:"African",San:"African",BiakaPygmy:"African",MbutiPygmy:"African",
  MXL:"American",PUR:"American",CLM:"American",PEL:"American",Maya:"American",
  Pima:"American",Colombian:"American",Karitiana:"American",Surui:"American",
  GIH:"SouthAsian",PJL:"SouthAsian",BEB:"SouthAsian",STU:"SouthAsian",ITU:"SouthAsian",
  Balochi:"SouthAsian",Brahui:"SouthAsian",Makrani:"SouthAsian",Sindhi:"SouthAsian",
  Pathan:"SouthAsian",Burusho:"SouthAsian",Hazara:"SouthAsian",Uygur:"SouthAsian",Kalash:"SouthAsian",
  Druze:"MiddleEastern",Palestinian:"MiddleEastern",Bedouin:"MiddleEastern",
  BedouinB:"MiddleEastern",Mozabite:"MiddleEastern",
  Papuan:"Oceanian",PapuanHighlands:"Oceanian",PapuanSepik:"Oceanian",
  Bougainville:"Oceanian",Melanesian:"Oceanian",
};

/* ═══════════════════════════════════════════════════════════════
   PCA Scatter Plot (Canvas)
   ═══════════════════════════════════════════════════════════════ */
function PCAPlot({ pca, sampleName, extraQueries }) {
  const canvasRef = useRef(null);
  const [axes, setAxes] = useState([0, 1]); // PC indices
  const pcLabels = ["PC1", "PC2", "PC3", "PC4"];

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || !pca?.query || !pca?.ref_samples) return;
    const ctx = canvas.getContext("2d");
    const W = canvas.width, H = canvas.height;
    const pad = 40;

    ctx.clearRect(0, 0, W, H);
    ctx.fillStyle = "#0d1117";
    ctx.fillRect(0, 0, W, H);

    const pcKey = (i) => `pc${i + 1}`;
    const axX = axes[0], axY = axes[1];

    // Collect all points to determine scale
    const allX = [], allY = [];
    for (const s of pca.ref_samples) {
      allX.push(s[pcKey(axX)]);
      allY.push(s[pcKey(axY)]);
    }
    allX.push(pca.query[pcKey(axX)]);
    allY.push(pca.query[pcKey(axY)]);
    if (extraQueries) {
      for (const eq of extraQueries) {
        if (eq.pca?.query) {
          allX.push(eq.pca.query[pcKey(axX)]);
          allY.push(eq.pca.query[pcKey(axY)]);
        }
      }
    }

    const minX = Math.min(...allX), maxX = Math.max(...allX);
    const minY = Math.min(...allY), maxY = Math.max(...allY);
    const rangeX = maxX - minX || 1, rangeY = maxY - minY || 1;
    const scale = (v, min, range, size) => pad + ((v - min) / range) * (size - 2 * pad);

    // Draw grid lines
    ctx.strokeStyle = "#21262d";
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
      const x = pad + (i / 4) * (W - 2 * pad);
      const y = pad + (i / 4) * (H - 2 * pad);
      ctx.beginPath(); ctx.moveTo(x, pad); ctx.lineTo(x, H - pad); ctx.stroke();
      ctx.beginPath(); ctx.moveTo(pad, y); ctx.lineTo(W - pad, y); ctx.stroke();
    }

    // Draw reference samples
    for (const s of pca.ref_samples) {
      const x = scale(s[pcKey(axX)], minX, rangeX, W);
      const y = H - scale(s[pcKey(axY)], minY, rangeY, H);
      ctx.globalAlpha = 0.5;
      ctx.fillStyle = groupColor(s.group);
      ctx.beginPath();
      ctx.arc(x, y, 3, 0, Math.PI * 2);
      ctx.fill();
    }

    // Draw centroids as larger semi-transparent circles
    ctx.globalAlpha = 0.2;
    if (pca.centroids) {
      for (const [g, c] of Object.entries(pca.centroids)) {
        const x = scale(c[pcKey(axX)], minX, rangeX, W);
        const y = H - scale(c[pcKey(axY)], minY, rangeY, H);
        ctx.fillStyle = groupColor(g);
        ctx.beginPath();
        ctx.arc(x, y, 16, 0, Math.PI * 2);
        ctx.fill();
      }
    }

    ctx.globalAlpha = 1.0;

    // Draw extra query samples (for comparison view)
    if (extraQueries) {
      for (const eq of extraQueries) {
        if (!eq.pca?.query) continue;
        const x = scale(eq.pca.query[pcKey(axX)], minX, rangeX, W);
        const y = H - scale(eq.pca.query[pcKey(axY)], minY, rangeY, H);
        ctx.fillStyle = "#fff";
        ctx.strokeStyle = "#e6edf3";
        ctx.lineWidth = 2;
        ctx.beginPath(); ctx.arc(x, y, 7, 0, Math.PI * 2); ctx.fill(); ctx.stroke();
        // Label
        ctx.fillStyle = "#c9d1d9";
        ctx.font = "11px -apple-system, sans-serif";
        ctx.fillText(eq.sample_name, x + 10, y + 4);
      }
    }

    // Draw query sample (prominent)
    const qx = scale(pca.query[pcKey(axX)], minX, rangeX, W);
    const qy = H - scale(pca.query[pcKey(axY)], minY, rangeY, H);

    // Pulsing ring
    ctx.strokeStyle = "#f0883e";
    ctx.lineWidth = 2;
    ctx.beginPath(); ctx.arc(qx, qy, 12, 0, Math.PI * 2); ctx.stroke();

    // Filled dot
    ctx.fillStyle = "#f0883e";
    ctx.beginPath(); ctx.arc(qx, qy, 6, 0, Math.PI * 2); ctx.fill();

    // Label
    ctx.fillStyle = "#f0883e";
    ctx.font = "bold 12px -apple-system, sans-serif";
    ctx.fillText(sampleName || "You", qx + 14, qy + 4);

    // Axis labels
    ctx.fillStyle = "#8b949e";
    ctx.font = "12px -apple-system, sans-serif";
    ctx.textAlign = "center";
    ctx.fillText(pcLabels[axX], W / 2, H - 8);
    ctx.save();
    ctx.translate(12, H / 2);
    ctx.rotate(-Math.PI / 2);
    ctx.fillText(pcLabels[axY], 0, 0);
    ctx.restore();
    ctx.textAlign = "start";

    // Legend
    const groups = [...new Set(pca.ref_samples.map((s) => s.group))];
    const legX = W - 140, legY = 20;
    ctx.font = "11px -apple-system, sans-serif";
    groups.forEach((g, i) => {
      ctx.fillStyle = groupColor(g);
      ctx.beginPath(); ctx.arc(legX, legY + i * 16, 4, 0, Math.PI * 2); ctx.fill();
      ctx.fillStyle = "#8b949e";
      ctx.fillText(g, legX + 10, legY + i * 16 + 4);
    });

  }, [pca, axes, extraQueries]);

  if (!pca?.query || !pca?.ref_samples) return null;

  return (
    <div style={s.card}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
        <div style={s.sectionTitle}>PCA — Principal Component Analysis</div>
        <div style={{ display: "flex", gap: 6 }}>
          {[[0,1],[0,2],[1,2],[0,3]].map(([a,b]) => (
            <button key={`${a}-${b}`}
              style={{ ...s.btn, padding: "4px 10px", fontSize: 11,
                ...(axes[0] === a && axes[1] === b ? { background: "#21262d", color: "#e6edf3" } : { background: "transparent", color: "#8b949e", border: "1px solid #30363d" }) }}
              onClick={() => setAxes([a, b])}>
              {pcLabels[a]} vs {pcLabels[b]}
            </button>
          ))}
        </div>
      </div>
      <canvas ref={canvasRef} width={760} height={500} style={{ width: "100%", borderRadius: 8, border: "1px solid #21262d" }} />
      <div style={{ fontSize: 12, color: "#8b949e", marginTop: 8 }}>
        Each dot is a reference sample from the gnomAD HGDP+1kGP panel. Your sample is the orange marker.
        PCA captures the major axes of genetic variation.
      </div>
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════
   Composition Chart
   ═══════════════════════════════════════════════════════════════ */
function CompositionChart({ proportions }) {
  if (!proportions) return null;
  const sorted = Object.entries(proportions).sort((a, b) => b[1] - a[1]);
  const visible = sorted.filter(([, v]) => v > 0.005);

  return (
    <div>
      {/* Composition bar */}
      <div style={s.compBar}>
        {visible.map(([g, v]) => (
          <div key={g} style={{ ...s.compSegment, width: `${v * 100}%`, background: groupColor(g) }}
            title={`${g}: ${(v * 100).toFixed(1)}%`}>
            {v > 0.06 ? `${(v * 100).toFixed(0)}%` : ""}
          </div>
        ))}
      </div>
      {/* Proportion cards */}
      <div style={s.compGrid}>
        {visible.map(([g, v]) => (
          <div key={g} style={s.compCard}>
            <div style={{ ...s.compDot, background: groupColor(g) }} />
            <div>
              <div style={s.compPct}>{(v * 100).toFixed(1)}%</div>
              <div style={s.compLabel}>{g}</div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════
   Ancestry Context (group detail cards)
   ═══════════════════════════════════════════════════════════════ */
function AncestryContext({ proportions }) {
  if (!proportions) return null;
  const sorted = Object.entries(proportions).sort((a, b) => b[1] - a[1]).filter(([, v]) => v > 0.005);

  return (
    <div style={s.card}>
      <div style={s.sectionTitle}>Ancestry Context</div>
      {sorted.map(([g, v]) => {
        const info = GROUP_INFO[g] || {};
        return (
          <div key={g} style={{ padding: "14px 0", borderBottom: "1px solid #21262d" }}>
            <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
              <span style={{ fontSize: 20 }}>{info.emoji || "🌐"}</span>
              <div>
                <span style={{ fontSize: 14, fontWeight: 600, color: groupColor(g) }}>{g}</span>
                <span style={{ fontSize: 12, color: "#8b949e", marginLeft: 8 }}>{info.region || ""}</span>
              </div>
              <span style={{ marginLeft: "auto", fontSize: 16, fontWeight: 700, color: "#e6edf3" }}>{(v * 100).toFixed(1)}%</span>
            </div>
            <div style={{ fontSize: 13, color: "#8b949e", lineHeight: 1.6, paddingLeft: 30 }}>{info.desc || ""}</div>
          </div>
        );
      })}
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════
   Population Breakdown (detailed pop-level results)
   ═══════════════════════════════════════════════════════════════ */
function PopulationBreakdown({ popProportions, proportions }) {
  const [showAll, setShowAll] = useState(false);
  if (!popProportions || Object.keys(popProportions).length === 0) return null;

  const sorted = Object.entries(popProportions).sort((a, b) => b[1] - a[1]);
  const display = showAll ? sorted : sorted.filter(([, v]) => v > 0.005);

  return (
    <div style={s.card}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div style={s.sectionTitle}>Population-Level Breakdown</div>
        <button style={{ ...s.btn, ...s.btnSecondary, padding: "4px 12px", fontSize: 12 }}
          onClick={() => setShowAll(!showAll)}>{showAll ? "Show significant" : "Show all"}</button>
      </div>
      <div style={{ fontSize: 12, color: "#8b949e", marginBottom: 16 }}>
        NNLS decomposition across {Object.keys(popProportions).length} reference populations
      </div>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ borderBottom: "1px solid #30363d" }}>
            <th style={{ textAlign: "left", padding: "8px 12px", fontSize: 12, color: "#8b949e", fontWeight: 500 }}>Population</th>
            <th style={{ textAlign: "left", padding: "8px 12px", fontSize: 12, color: "#8b949e", fontWeight: 500 }}>Group</th>
            <th style={{ textAlign: "right", padding: "8px 12px", fontSize: 12, color: "#8b949e", fontWeight: 500 }}>%</th>
            <th style={{ textAlign: "left", padding: "8px 12px", fontSize: 12, color: "#8b949e", fontWeight: 500, width: "40%" }}></th>
          </tr>
        </thead>
        <tbody>
          {display.map(([pop, val]) => {
            const group = POP_TO_GROUP[pop] || "Unknown";
            return (
              <tr key={pop} style={{ borderBottom: "1px solid #161b22" }}>
                <td style={{ padding: "6px 12px", fontSize: 13, color: "#e6edf3" }}>{pop}</td>
                <td style={{ padding: "6px 12px", fontSize: 12 }}>
                  <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
                    <span style={{ width: 8, height: 8, borderRadius: "50%", background: groupColor(group), display: "inline-block" }} />
                    <span style={{ color: "#8b949e" }}>{group}</span>
                  </span>
                </td>
                <td style={{ padding: "6px 12px", fontSize: 13, color: "#e6edf3", textAlign: "right", fontWeight: 500 }}>
                  {(val * 100).toFixed(1)}
                </td>
                <td style={{ padding: "6px 12px" }}>
                  <div style={{ height: 6, background: "#21262d", borderRadius: 3, overflow: "hidden" }}>
                    <div style={{ height: "100%", width: `${Math.min(val * 100 * 2, 100)}%`, background: groupColor(group), borderRadius: 3 }} />
                  </div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════
   Flags
   ═══════════════════════════════════════════════════════════════ */
function Flags({ flags }) {
  if (!flags || flags.length === 0) return null;
  return (
    <div style={s.card}>
      <div style={s.sectionTitle}>Flags & Insights</div>
      {flags.map((f, i) => (
        <div key={i} style={s.flagBox}>
          <span style={{ fontSize: 18 }}>🔍</span>
          <div style={{ fontSize: 13, color: "#c9d1d9", lineHeight: 1.6 }}>{f}</div>
        </div>
      ))}
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════
   ROH (Runs of Homozygosity)
   ═══════════════════════════════════════════════════════════════ */
function ROH({ roh }) {
  if (!roh) return null;
  return (
    <div style={s.card}>
      <div style={s.sectionTitle}>Runs of Homozygosity (ROH)</div>
      <div style={s.rohCard}>
        <div>
          <div style={s.rohVal}>{typeof roh.total_mb === "number" ? roh.total_mb.toFixed(1) : roh.total_mb ?? "—"}</div>
          <div style={s.rohLabel}>Total ROH (Mb)</div>
        </div>
        <div>
          <div style={s.rohVal}>{roh.n_segments ?? "—"}</div>
          <div style={s.rohLabel}>Segments</div>
        </div>
        <div>
          <div style={s.rohVal}>{typeof roh.avg_kb === "number" ? roh.avg_kb.toFixed(0) : roh.avg_kb ?? "—"}</div>
          <div style={s.rohLabel}>Avg Segment (kb)</div>
        </div>
        <div>
          <div style={{ ...s.rohVal, color: roh.bottleneck ? "#d29922" : "#3fb950" }}>
            {roh.bottleneck ? "Yes" : "No"}
          </div>
          <div style={s.rohLabel}>Bottleneck Signal</div>
        </div>
      </div>
      <div style={{ fontSize: 12, color: "#8b949e", marginTop: 12, lineHeight: 1.6 }}>
        ROH reflect identical-by-descent segments inherited from shared ancestors. Higher values can
        indicate founder effects, endogamy, or recent consanguinity. Typical ranges: outbred
        populations ~0–50 Mb; founder populations (e.g. Finnish, Ashkenazi) ~50–200 Mb.
      </div>
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════
   Technical Details
   ═══════════════════════════════════════════════════════════════ */
function TechDetails({ result, job }) {
  const [open, setOpen] = useState(false);
  if (!result) return null;
  return (
    <div style={{ ...s.card, marginTop: 20 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", cursor: "pointer" }} onClick={() => setOpen(!open)}>
        <div style={{ fontSize: 14, fontWeight: 600, color: "#8b949e" }}>Technical Details</div>
        <span style={{ color: "#8b949e", fontSize: 12 }}>{open ? "▲ Hide" : "▼ Show"}</span>
      </div>
      {open && (
        <div style={{ marginTop: 16 }}>
          {[
            ["Sample", result.sample_name],
            ["Panel", result.panel],
            ["Variants Extracted", result.variants_extracted?.toLocaleString()],
            ["Variants Used (merged)", result.variants_used?.toLocaleString()],
            ["Input Type", result.input_type],
          ].map(([k, v]) => v != null && (
            <div key={k} style={s.techRow}>
              <span style={{ color: "#8b949e" }}>{k}</span>
              <span>{v}</span>
            </div>
          ))}
          {job?.created_at && <div style={s.techRow}><span style={{ color: "#8b949e" }}>Started</span><span>{new Date(job.created_at).toLocaleString()}</span></div>}
          {job?.completed_at && <div style={s.techRow}><span style={{ color: "#8b949e" }}>Completed</span><span>{new Date(job.completed_at).toLocaleString()}</span></div>}
        </div>
      )}
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════
   Ancestry Signature Heatmap
   ═══════════════════════════════════════════════════════════════ */
function SignaturesSection({ signatures }) {
  if (!signatures || signatures.length === 0) return null;

  // Sort by strength descending
  const sorted = [...signatures].sort((a, b) => (b.strength || 0) - (a.strength || 0));

  return (
    <div style={s.card}>
      <div style={s.sectionTitle}>Ancestry Signatures</div>
      <div style={{ fontSize: 12, color: "#8b949e", marginBottom: 16 }}>
        Characteristic patterns detected in your ancestry composition
      </div>
      {sorted.map((sig, i) => (
        <div key={i} style={{
          padding: "12px 16px", marginBottom: 8,
          background: "#0d1117", border: "1px solid #21262d", borderRadius: 8,
          borderLeft: `3px solid ${sig.strength > 0.7 ? "#3fb950" : sig.strength > 0.4 ? "#d29922" : "#8b949e"}`,
        }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 4 }}>
            <span style={{ fontSize: 14, fontWeight: 600, color: "#e6edf3" }}>{sig.label}</span>
            <span style={{
              fontSize: 11, padding: "2px 8px", borderRadius: 10,
              background: sig.strength > 0.7 ? "#238636" : sig.strength > 0.4 ? "#9e6a03" : "#30363d",
              color: "#e6edf3",
            }}>
              {sig.strength > 0.7 ? "Strong" : sig.strength > 0.4 ? "Moderate" : "Weak"}
            </span>
          </div>
          <div style={{ fontSize: 12, color: "#8b949e", lineHeight: 1.5 }}>{sig.description}</div>
          {sig.components && (
            <div style={{ marginTop: 8, display: "flex", gap: 8, flexWrap: "wrap" }}>
              {sig.components.map((c, j) => (
                <span key={j} style={{
                  fontSize: 11, padding: "2px 8px", borderRadius: 4,
                  background: "#161b22", border: "1px solid #30363d", color: "#c9d1d9",
                }}>
                  {c.group}: {(c.proportion * 100).toFixed(1)}%
                </span>
              ))}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════
   World Map (SVG)
   ═══════════════════════════════════════════════════════════════ */
function WorldMap({ proportions }) {
  if (!proportions) return null;
  const sorted = Object.entries(proportions).sort((a, b) => b[1] - a[1]).filter(([, v]) => v > 0.01);
  if (sorted.length === 0) return null;

  // Simple equirectangular projection
  const W = 760, H = 380;
  const project = ([lon, lat]) => [(lon + 180) / 360 * W, (90 - lat) / 180 * H];

  return (
    <div style={s.card}>
      <div style={s.sectionTitle}>Geographic Distribution</div>
      <svg viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", background: "#0d1117", borderRadius: 8, border: "1px solid #21262d" }}>
        {/* World outline — simplified continental shapes */}
        <rect x={0} y={0} width={W} height={H} fill="#0d1117" />
        {/* Grid lines */}
        {[-60, -30, 0, 30, 60].map((lat) => {
          const [, y] = project([0, lat]);
          return <line key={`lat${lat}`} x1={0} y1={y} x2={W} y2={y} stroke="#161b22" strokeWidth={0.5} />;
        })}
        {[-120, -60, 0, 60, 120].map((lon) => {
          const [x] = project([lon, 0]);
          return <line key={`lon${lon}`} x1={x} y1={0} x2={x} y2={H} stroke="#161b22" strokeWidth={0.5} />;
        })}
        {/* Equator */}
        {(() => { const [, y] = project([0, 0]); return <line x1={0} y1={y} x2={W} y2={y} stroke="#21262d" strokeWidth={1} strokeDasharray="4 4" />; })()}
        {/* Group markers */}
        {sorted.map(([g, v]) => {
          const geo = GROUP_GEO[g];
          if (!geo) return null;
          const [cx, cy] = project(geo);
          const r = Math.max(8, Math.sqrt(v) * 60);
          return (
            <g key={g}>
              <circle cx={cx} cy={cy} r={r} fill={groupColor(g)} opacity={0.25} />
              <circle cx={cx} cy={cy} r={Math.max(4, r * 0.4)} fill={groupColor(g)} opacity={0.7} />
              <text x={cx} y={cy - r - 6} textAnchor="middle" fill="#c9d1d9" fontSize={11} fontWeight={600}>
                {g} {(v * 100).toFixed(0)}%
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════
   Overview Tab
   ═══════════════════════════════════════════════════════════════ */
function OverviewTab({ refStatus, refDetail, onStartAnalysis, history, viewJob }) {
  const completedJobs = history.filter((h) => h.status === "complete" && h.result_summary);

  return (
    <div>
      {completedJobs.length > 0 && (
        <div style={s.card}>
          <div style={{ ...s.sectionTitle, marginTop: 0 }}>Recent Results</div>
          {completedJobs.slice(0, 5).map((h) => (
            <div key={h.job_id}
              style={{ display: "flex", alignItems: "center", gap: 12, padding: "10px 0", borderBottom: "1px solid #21262d", cursor: "pointer" }}
              onClick={() => viewJob(h.job_id)}>
              <span style={{ width: 8, height: 8, borderRadius: "50%", background: "#3fb950", flexShrink: 0 }} />
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ fontSize: 14, fontWeight: 500, color: "#e6edf3" }}>{h.sample_name}</div>
                <div style={{ fontSize: 12, color: "#8b949e" }}>
                  {h.result_summary.primary} ({fmtPct(h.result_summary.primary_pct)}%)
                  {h.result_summary.is_admixed && " · Admixed"}
                </div>
              </div>
              <span style={{ fontSize: 12, color: "#8b949e" }}>{timeAgo(h.created_at)}</span>
            </div>
          ))}
        </div>
      )}

      {refStatus && (
        <div style={s.card}>
          <div style={{ ...s.sectionTitle, marginTop: 0 }}>Reference Panel Status</div>
          <div style={{
            display: "flex", alignItems: "center", gap: 8, marginBottom: 16,
            padding: "8px 12px", borderRadius: 6,
            background: refStatus.ready ? "#238636" + "22" : "#f85149" + "22",
            border: `1px solid ${refStatus.ready ? "#238636" : "#f85149"}44`,
          }}>
            <span style={{ fontSize: 16 }}>{refStatus.ready ? "✓" : "✗"}</span>
            <span style={{ fontSize: 14, color: refStatus.ready ? "#3fb950" : "#f85149" }}>
              {refStatus.ready ? "Reference panel ready" : "Reference panel not ready"}
            </span>
          </div>

          {refDetail && (
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(140px, 1fr))", gap: 12 }}>
              {[
                ["Variants", refDetail.stats?.variant_count?.toLocaleString() || "—"],
                ["Samples", refDetail.stats?.sample_count?.toLocaleString() || "—"],
                ["Populations", refDetail.stats?.population_count || "—"],
                ["Groups", refDetail.stats?.group_count || "—"],
                ["Size", `${refDetail.stats?.total_size_gb || "—"} GB`],
              ].map(([label, value]) => (
                <div key={label} style={s.statBox}>
                  <div style={{ fontSize: 18, fontWeight: 700, color: "#e6edf3" }}>{value}</div>
                  <div style={{ fontSize: 11, color: "#8b949e", marginTop: 2 }}>{label}</div>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════
   Compare Tab
   ═══════════════════════════════════════════════════════════════ */
function CompareTab({ history, loadHistory }) {
  const [selected, setSelected] = useState(new Set());
  const [comparison, setComparison] = useState(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => { loadHistory(); }, []);

  const completedJobs = history.filter((h) => h.status === "complete" && h.has_result);

  async function runCompare() {
    if (selected.size < 2) return;
    setLoading(true);
    try {
      const data = await api(`/jobs/compare?ids=${[...selected].join(",")}`);
      setComparison(data.comparisons);
    } catch (e) { toast(e.message, "error"); }
    finally { setLoading(false); }
  }

  function toggle(id) {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  }

  if (comparison) {
    // All groups across all results
    const allGroups = new Set();
    for (const r of comparison) Object.keys(r.proportions).forEach((g) => allGroups.add(g));
    const groups = [...allGroups].sort();

    // First result's PCA data for overlay
    const basePCA = comparison[0]?.pca;
    const extraQueries = comparison.slice(1).map((c) => ({ pca: c.pca, sample_name: c.sample_name }));

    return (
      <div>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
          <div style={s.sectionTitle}>Comparison ({comparison.length} samples)</div>
          <button style={{ ...s.btn, ...s.btnSecondary }} onClick={() => setComparison(null)}>Back</button>
        </div>

        {/* Stacked bars */}
        <div style={s.card}>
          {comparison.map((r) => (
            <div key={r.job_id} style={{ marginBottom: 16 }}>
              <div style={{ fontSize: 13, fontWeight: 600, color: "#e6edf3", marginBottom: 4 }}>
                {r.sample_name}
                <span style={{ fontSize: 12, fontWeight: 400, color: "#8b949e", marginLeft: 8 }}>
                  {r.primary} ({fmtPct(r.primary_pct)}%)
                </span>
              </div>
              <div style={s.compBar}>
                {Object.entries(r.proportions).sort((a, b) => b[1] - a[1]).map(([g, v]) => (
                  <div key={g} style={{ ...s.compSegment, width: `${v * 100}%`, background: groupColor(g) }}
                    title={`${g}: ${(v * 100).toFixed(1)}%`}>
                    {v > 0.08 ? `${(v * 100).toFixed(0)}%` : ""}
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>

        {/* Table */}
        <div style={s.card}>
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ borderBottom: "2px solid #30363d" }}>
                  <th style={{ textAlign: "left", padding: "8px 12px", fontSize: 12, color: "#8b949e" }}>Sample</th>
                  {groups.map((g) => (
                    <th key={g} style={{ textAlign: "right", padding: "8px 6px", fontSize: 11, color: groupColor(g) }}>{g}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {comparison.map((r) => (
                  <tr key={r.job_id} style={{ borderBottom: "1px solid #21262d" }}>
                    <td style={{ padding: "8px 12px", fontSize: 13, color: "#e6edf3", fontWeight: 500, whiteSpace: "nowrap" }}>{r.sample_name}</td>
                    {groups.map((g) => {
                      const v = r.proportions[g] || 0;
                      return (
                        <td key={g} style={{ textAlign: "right", padding: "8px 6px", fontSize: 13, color: v > 0.01 ? "#e6edf3" : "#30363d" }}>
                          {v > 0.005 ? (v * 100).toFixed(1) : "—"}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* PCA overlay */}
        {basePCA && <PCAPlot pca={basePCA} sampleName={comparison[0]?.sample_name} extraQueries={extraQueries} />}
      </div>
    );
  }

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
        <div style={s.sectionTitle}>Compare Samples</div>
        <button style={{ ...s.btn, ...s.btnPrimary, ...(selected.size < 2 || loading ? s.btnDisabled : {}) }}
          disabled={selected.size < 2 || loading}
          onClick={runCompare}>
          {loading ? "Comparing..." : `Compare ${selected.size} Selected`}
        </button>
      </div>
      {completedJobs.length < 2 && (
        <div style={{ color: "#8b949e", textAlign: "center", padding: 40 }}>
          Need at least 2 completed analyses to compare.
        </div>
      )}
      {completedJobs.map((h) => (
        <div key={h.job_id} style={{
          ...s.historyRow,
          border: selected.has(h.job_id) ? "1px solid #58a6ff" : "1px solid #21262d",
          background: selected.has(h.job_id) ? "#161b2288" : "#161b22",
        }} onClick={() => toggle(h.job_id)}>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <input type="checkbox" checked={selected.has(h.job_id)} readOnly
              style={{ accentColor: "#58a6ff" }} />
            <div>
              <div style={{ fontSize: 14, fontWeight: 500, color: "#e6edf3" }}>{h.sample_name}</div>
              <div style={{ fontSize: 12, color: "#8b949e" }}>
                {h.result_summary?.primary} ({fmtPct(h.result_summary?.primary_pct)}%)
              </div>
            </div>
          </div>
          <span style={{ fontSize: 12, color: "#8b949e" }}>{timeAgo(h.created_at)}</span>
        </div>
      ))}
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════
   History Tab
   ═══════════════════════════════════════════════════════════════ */
function HistoryTab({ history, loadHistory, viewJob, goAnalyze }) {
  const autoRef = useRef(null);
  const hasRunning = history.some((h) => h.status === "running" || h.status === "queued");

  useEffect(() => {
    loadHistory();
  }, []);

  // Auto-refresh while jobs are running
  useEffect(() => {
    if (hasRunning) {
      autoRef.current = setInterval(loadHistory, 5000);
    }
    return () => clearInterval(autoRef.current);
  }, [hasRunning]);

  async function deleteJob(e, jobId, name) {
    e.stopPropagation();
    if (!confirm(`Delete analysis for "${name}"? This cannot be undone.`)) return;
    try {
      await api(`/jobs/${jobId}`, { method: "DELETE" });
      loadHistory();
      toast(`Deleted ${name}`, "info");
    } catch { toast("Delete failed", "error"); }
  }

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{ ...s.sectionTitle, margin: 0 }}>Analysis History</div>
          {hasRunning && <span style={{ fontSize: 12, color: "#d29922", animation: "none" }}>auto-refreshing...</span>}
        </div>
        <button style={{ ...s.btn, ...s.btnSecondary, padding: "6px 14px", fontSize: 13 }} onClick={loadHistory}>Refresh</button>
      </div>
      {history.length === 0 && (
        <div style={{ color: "#8b949e", textAlign: "center", padding: 40 }}>
          No analyses yet.<br />
          <button style={{ ...s.btn, ...s.btnPrimary, marginTop: 16 }} onClick={goAnalyze}>Start your first analysis</button>
        </div>
      )}
      {history.map((h) => (
        <div key={h.job_id} style={s.historyRow} onClick={() => viewJob(h.job_id)}>
          <div style={{ display: "flex", alignItems: "center", gap: 10, flex: 1, minWidth: 0 }}>
            <span style={{ width: 8, height: 8, borderRadius: "50%", flexShrink: 0,
              background: h.status === "complete" ? "#3fb950" : h.status === "failed" ? "#f85149" : "#d29922" }} />
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ fontSize: 14, fontWeight: 500, color: "#e6edf3" }}>{h.sample_name}</div>
              {h.status === "running" || h.status === "queued" ? (
                <div>
                  <div style={{ fontSize: 12, color: "#d29922", marginBottom: 4 }}>
                    {h.current_step || "Queued..."} — {Math.round(h.progress || 0)}%
                  </div>
                  <div style={{ height: 3, background: "#21262d", borderRadius: 2, overflow: "hidden", maxWidth: 200 }}>
                    <div style={{ height: "100%", width: `${h.progress || 0}%`, background: "#d29922", borderRadius: 2, transition: "width 0.3s" }} />
                  </div>
                </div>
              ) : (
                <div style={{ fontSize: 12, color: "#8b949e" }}>
                  {h.result_summary
                    ? `${h.result_summary.primary} (${fmtPct(h.result_summary.primary_pct)}%)${h.result_summary.is_admixed ? " · Admixed" : ""}`
                    : h.status === "failed" ? (h.error?.slice(0, 60) || "Failed") : h.current_step}
                </div>
              )}
            </div>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 12, flexShrink: 0 }}>
            <span style={{ fontSize: 12, color: "#8b949e" }}>{timeAgo(h.created_at)}</span>
            {(h.status === "complete" || h.status === "failed") && (
              <button
                style={{ background: "none", border: "none", color: "#8b949e", cursor: "pointer", fontSize: 14, padding: "2px 6px", borderRadius: 4 }}
                title="Delete"
                onClick={(e) => deleteJob(e, h.job_id, h.sample_name)}
              >
                ✕
              </button>
            )}
          </div>
        </div>
      ))}
    </div>
  );
}

function BatchAnalyze({ serverFiles, onQueued }) {
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);

  async function runBatch() {
    setLoading(true); setResult(null);
    try {
      const data = await api("/analyze/batch", { method: "POST" });
      setResult(data);
      if (data.total_queued > 0) {
        toast(`Queued ${data.total_queued} analyses`, "success");
        if (onQueued) onQueued();
      } else if (data.total_skipped > 0) {
        toast("All samples already analyzed!", "info");
      }
    } catch (e) { setResult({ error: e.message }); }
    finally { setLoading(false); }
  }

  return (
    <div style={{ ...s.card, background: "#0d1117", border: "1px solid #21262d" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div>
          <div style={{ fontSize: 14, fontWeight: 600, color: "#c9d1d9" }}>Batch Analysis</div>
          <div style={{ fontSize: 12, color: "#8b949e", marginTop: 2 }}>
            Analyze all {serverFiles.length} server files. Already-completed samples are skipped.
          </div>
        </div>
        <button style={{ ...s.btn, ...s.btnSecondary, ...(loading ? s.btnDisabled : {}) }}
          disabled={loading} onClick={runBatch}>
          {loading ? "Starting..." : `Analyze All (${serverFiles.length})`}
        </button>
      </div>
      {result && !result.error && (
        <div style={{ marginTop: 12, fontSize: 13, color: "#8b949e" }}>
          {result.total_queued > 0 && <span style={{ color: "#3fb950" }}>Queued {result.total_queued} analyses. </span>}
          {result.total_skipped > 0 && <span>Skipped {result.total_skipped} already-completed ({result.skipped.join(", ")}). </span>}
          {result.total_queued === 0 && result.total_skipped > 0 && <span>All samples already analyzed!</span>}
        </div>
      )}
      {result?.error && <div style={{ ...s.error, marginTop: 12 }}>{result.error}</div>}
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════
   Toast Notification System
   ═══════════════════════════════════════════════════════════════ */
let _toastId = 0;
let _setToasts = null;

function toast(message, type = "info") {
  if (!_setToasts) return;
  const id = ++_toastId;
  _setToasts((prev) => [...prev, { id, message, type }]);
  setTimeout(() => _setToasts((prev) => prev.filter((t) => t.id !== id)), 4000);
}

function ToastContainer({ toasts }) {
  if (!toasts.length) return null;
  const colors = { success: "#238636", error: "#f85149", info: "#58a6ff", warning: "#d29922" };
  return (
    <div style={{ position: "fixed", top: 16, right: 16, zIndex: 2000, display: "flex", flexDirection: "column", gap: 8, maxWidth: 360 }}>
      {toasts.map((t) => (
        <div key={t.id} style={{
          background: "#161b22", border: `1px solid ${colors[t.type] || colors.info}`,
          borderLeft: `3px solid ${colors[t.type] || colors.info}`,
          borderRadius: 8, padding: "10px 16px", fontSize: 13, color: "#c9d1d9",
          boxShadow: "0 4px 12px rgba(0,0,0,0.4)", animation: "fadeIn 0.2s ease",
        }}>
          {t.message}
        </div>
      ))}
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════
   Login Screen
   ═══════════════════════════════════════════════════════════════ */
function LoginScreen({ onLogin }) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e) {
    e.preventDefault();
    if (!email.trim() || !password) return;
    setLoading(true); setError(null);
    try {
      const res = await fetch(`${API}/auth/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email: email.trim(), password }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || "Login failed");
      onLogin(data.user);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div style={{
      minHeight: "100vh", background: "#0d1117", display: "flex",
      alignItems: "center", justifyContent: "center", padding: 16,
    }}>
      <div style={{
        background: "#161b22", border: "1px solid #30363d", borderRadius: 12,
        padding: "40px 36px", width: 380, maxWidth: "90vw",
      }}>
        <div style={{ textAlign: "center", marginBottom: 32 }}>
          <div style={{ fontSize: 48, marginBottom: 12 }}>🧬</div>
          <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700, color: "#e6edf3" }}>23andClaude</h1>
          <p style={{ margin: "6px 0 0", fontSize: 13, color: "#8b949e" }}>Ancestry Inference Platform</p>
        </div>

        <form onSubmit={handleSubmit}>
          <div style={{ marginBottom: 16 }}>
            <label style={{ display: "block", fontSize: 13, fontWeight: 500, color: "#c9d1d9", marginBottom: 6 }}>Email</label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="you@example.com"
              autoFocus
              style={{
                width: "100%", padding: "10px 14px", background: "#0d1117",
                border: "1px solid #30363d", borderRadius: 6, color: "#c9d1d9",
                fontSize: 14, fontFamily: "inherit", boxSizing: "border-box", outline: "none",
              }}
            />
          </div>
          <div style={{ marginBottom: 24 }}>
            <label style={{ display: "block", fontSize: 13, fontWeight: 500, color: "#c9d1d9", marginBottom: 6 }}>Password</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Enter your password"
              style={{
                width: "100%", padding: "10px 14px", background: "#0d1117",
                border: "1px solid #30363d", borderRadius: 6, color: "#c9d1d9",
                fontSize: 14, fontFamily: "inherit", boxSizing: "border-box", outline: "none",
              }}
            />
          </div>

          {error && (
            <div style={{
              background: "#f8514922", border: "1px solid #f8514944", borderRadius: 6,
              padding: "10px 14px", color: "#f85149", fontSize: 13, marginBottom: 16,
            }}>
              {error}
            </div>
          )}

          <button
            type="submit"
            disabled={loading || !email.trim() || !password}
            style={{
              width: "100%", padding: "12px 24px", borderRadius: 6, border: "none",
              fontSize: 14, fontWeight: 600, cursor: loading ? "wait" : "pointer",
              fontFamily: "inherit",
              background: loading ? "#21262d" : "#238636", color: "#fff",
              opacity: (!email.trim() || !password) ? 0.5 : 1,
            }}
          >
            {loading ? "Signing in..." : "Sign In"}
          </button>
        </form>
      </div>
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════
   Main App
   ═══════════════════════════════════════════════════════════════ */
export default function App() {
  const [toasts, setToasts] = useState([]);
  _setToasts = setToasts;

  // Auth state
  const [user, setUser] = useState(null);
  const [authChecked, setAuthChecked] = useState(false);

  // Check existing session on mount
  useEffect(() => {
    fetch(`${API}/auth/me`).then((r) => {
      if (r.ok) return r.json();
      throw new Error("not authed");
    }).then((u) => {
      setUser(u);
      setAuthChecked(true);
    }).catch(() => {
      setAuthChecked(true);
    });
  }, []);

  async function handleLogout() {
    try {
      await fetch(`${API}/auth/logout`, { method: "POST" });
    } catch {}
    setUser(null);
  }

  // Show nothing while checking auth
  if (!authChecked) {
    return (
      <div style={{ minHeight: "100vh", background: "#0d1117", display: "flex", alignItems: "center", justifyContent: "center" }}>
        <div style={{ color: "#8b949e", fontSize: 14 }}>Loading...</div>
      </div>
    );
  }

  // Show login if not authenticated
  if (!user) {
    return (
      <>
        <LoginScreen onLogin={(u) => setUser(u)} />
        <ToastContainer toasts={toasts} />
        <style>{responsiveCSS}</style>
      </>
    );
  }

  // Authenticated — render main app
  return <MainApp user={user} onLogout={handleLogout} toasts={toasts} />;
}


function MainApp({ user, onLogout, toasts }) {
  const [tab, setTab] = useState("home");
  const [refStatus, setRefStatus] = useState(null);
  const [refDetail, setRefDetail] = useState(null);
  const refReady = refStatus?.ready;
  const [serverFiles, setServerFiles] = useState([]);

  // Analyze form state
  const [view, setView] = useState("form");
  const [sampleName, setSampleName] = useState("");
  const [inputMode, setInputMode] = useState("path");
  const [file, setFile] = useState(null);
  const [filePath, setFilePath] = useState("");
  const [fastaPath, setFastaPath] = useState("");
  const [showFasta, setShowFasta] = useState(false);
  const [dragging, setDragging] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState(null);

  // Job tracking
  const [job, setJob] = useState(null);
  const [elapsed, setElapsed] = useState(0);
  const pollRef = useRef(null);
  const timerRef = useRef(null);
  const origTitle = useRef(document.title);

  // History
  const [history, setHistory] = useState([]);

  // Keyboard shortcuts overlay
  const [showShortcuts, setShowShortcuts] = useState(false);

  // Load initial data + handle URL hash routing
  useEffect(() => {
    api("/reference/status").then(setRefStatus).catch(() => {});
    api("/reference/detail").then(setRefDetail).catch(() => {});
    api("/server-files").then((d) => setServerFiles(d.files || [])).catch(() => {});
    loadHistory();

    // Auto-resume any running jobs on page load
    api("/jobs").then((d) => {
      const running = (d.jobs || []).find((j) => j.status === "running" || j.status === "queued");
      if (running) {
        api(`/jobs/${running.job_id}`).then((j) => {
          setJob(j); setTab("analyze"); setSampleName(j.sample_name || "");
          setView("progress");
          startPolling(running.job_id, j.sample_name || "Analysis");
        }).catch(() => {});
      }
    }).catch(() => {});

    // Hash routing: #results/JOB_ID, #compare, #history, #analyze
    const hash = window.location.hash.slice(1);
    if (hash.startsWith("results/")) {
      const jobId = hash.split("/")[1];
      if (jobId) viewJob(jobId);
    } else if (hash === "compare") {
      setTab("compare");
    } else if (hash === "history") {
      setTab("history");
    } else if (hash === "analyze") {
      setTab("analyze");
    }
  }, []);

  useEffect(() => {
    const p = filePath.toLowerCase();
    setShowFasta(p.endsWith(".bam") || p.endsWith(".cram"));
  }, [filePath]);

  // Keyboard shortcuts
  useEffect(() => {
    function onKey(e) {
      // Ignore when typing in inputs
      if (e.target.tagName === "INPUT" || e.target.tagName === "TEXTAREA" || e.target.tagName === "SELECT") return;

      if (e.key === "?") { setShowShortcuts((v) => !v); return; }
      if (e.key === "Escape") { setShowShortcuts(false); return; }

      // Number keys for tabs: 1=Overview, 2=Analyze, 3=Compare, 4=History
      if (e.key === "1") { setTab("home"); window.location.hash = ""; }
      if (e.key === "2") { setTab("analyze"); window.location.hash = "analyze"; }
      if (e.key === "3") { setTab("compare"); window.location.hash = "compare"; }
      if (e.key === "4") { setTab("history"); window.location.hash = "history"; loadHistory(); }

      // N = new analysis
      if (e.key === "n" || e.key === "N") { setTab("analyze"); resetForm(); window.location.hash = "analyze"; }

      // R = refresh history
      if (e.key === "r" || e.key === "R") { loadHistory(); }
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const loadHistory = useCallback(() => {
    api("/jobs").then((d) => setHistory(d.jobs || [])).catch(() => {});
  }, []);

  function resetForm() {
    setView("form"); setJob(null); setError(null); setElapsed(0);
    clearInterval(pollRef.current); clearInterval(timerRef.current);
    document.title = origTitle.current;
  }

  async function handleSubmit() {
    if (!sampleName.trim()) { setError("Sample name is required"); return; }
    if (inputMode === "upload" && !file) { setError("Please select a VCF/gVCF file"); return; }
    if (inputMode === "path" && !filePath.trim()) { setError("Please select or enter a file path"); return; }

    setSubmitting(true); setError(null);
    try {
      const fd = new FormData();
      fd.append("sample_name", sampleName.trim());
      if (inputMode === "upload") {
        fd.append("file", file);
      } else {
        fd.append("file_path", filePath.trim());
        if (fastaPath.trim()) fd.append("fasta_path", fastaPath.trim());
      }

      const data = await api("/analyze", { method: "POST", body: fd });
      setView("progress"); setElapsed(0);
      startPolling(data.job_id, sampleName.trim());
    } catch (e) { setError(e.message); }
    finally { setSubmitting(false); }
  }

  function downloadResult() {
    if (!job?.result) return;
    const blob = new Blob([JSON.stringify(job.result, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = `${job.result.sample_name || "ancestry"}_result.json`; a.click();
    URL.revokeObjectURL(url);
  }

  function exportPNG() {
    if (!job?.result) return;
    const r = job.result;
    const W = 800, H = 500;
    const cvs = document.createElement("canvas");
    cvs.width = W; cvs.height = H;
    const ctx = cvs.getContext("2d");

    // Background
    ctx.fillStyle = "#0d1117";
    ctx.fillRect(0, 0, W, H);

    // Header
    ctx.fillStyle = "#e6edf3";
    ctx.font = "bold 24px -apple-system, BlinkMacSystemFont, sans-serif";
    ctx.fillText(`${r.sample_name} — Ancestry`, 32, 44);
    ctx.fillStyle = "#8b949e";
    ctx.font = "13px -apple-system, sans-serif";
    ctx.fillText(`23andClaude · gnomAD HGDP+1kGP · ${r.variants_used?.toLocaleString() || "?"} variants`, 32, 66);

    // Composition bar
    const barY = 90, barH = 36;
    const sorted = Object.entries(r.proportions).sort((a, b) => b[1] - a[1]);
    let bx = 32;
    const barW = W - 64;
    ctx.save();
    ctx.beginPath();
    ctx.roundRect(32, barY, barW, barH, 6);
    ctx.clip();
    for (const [g, v] of sorted) {
      const segW = v * barW;
      ctx.fillStyle = groupColor(g);
      ctx.fillRect(bx, barY, segW, barH);
      if (v > 0.06) {
        ctx.fillStyle = "#fff";
        ctx.font = "bold 12px -apple-system, sans-serif";
        ctx.textAlign = "center";
        ctx.fillText(`${(v * 100).toFixed(0)}%`, bx + segW / 2, barY + barH / 2 + 4);
        ctx.textAlign = "start";
      }
      bx += segW;
    }
    ctx.restore();

    // Proportion cards
    const cardY = 148;
    const cols = 4;
    const cardW = (barW - (cols - 1) * 12) / cols;
    const visible = sorted.filter(([, v]) => v > 0.005);
    visible.forEach(([g, v], i) => {
      const col = i % cols, row = Math.floor(i / cols);
      const cx = 32 + col * (cardW + 12), cy = cardY + row * 64;

      ctx.fillStyle = "#161b22";
      ctx.strokeStyle = "#30363d";
      ctx.lineWidth = 1;
      ctx.beginPath(); ctx.roundRect(cx, cy, cardW, 52, 6); ctx.fill(); ctx.stroke();

      // Dot
      ctx.fillStyle = groupColor(g);
      ctx.beginPath(); ctx.arc(cx + 16, cy + 26, 6, 0, Math.PI * 2); ctx.fill();

      // Text
      ctx.fillStyle = "#e6edf3";
      ctx.font = "bold 16px -apple-system, sans-serif";
      ctx.fillText(`${(v * 100).toFixed(1)}%`, cx + 30, cy + 24);
      ctx.fillStyle = "#8b949e";
      ctx.font = "11px -apple-system, sans-serif";
      ctx.fillText(g, cx + 30, cy + 40);
    });

    // Flags
    const flagY = cardY + Math.ceil(visible.length / cols) * 64 + 16;
    if (r.flags?.length) {
      ctx.fillStyle = "#8b949e";
      ctx.font = "12px -apple-system, sans-serif";
      r.flags.slice(0, 3).forEach((f, i) => {
        ctx.fillText(`🔍 ${f}`, 32, flagY + i * 20);
      });
    }

    // Footer
    ctx.fillStyle = "#30363d";
    ctx.fillRect(0, H - 36, W, 36);
    ctx.fillStyle = "#8b949e";
    ctx.font = "11px -apple-system, sans-serif";
    ctx.fillText("Generated by 23andClaude Ancestry · 23andclaude.com", 32, H - 14);
    ctx.textAlign = "end";
    ctx.fillText(new Date().toLocaleDateString(), W - 32, H - 14);
    ctx.textAlign = "start";

    // Download
    cvs.toBlob((blob) => {
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url; a.download = `${r.sample_name || "ancestry"}_results.png`; a.click();
      URL.revokeObjectURL(url);
    });
  }

  function startPolling(jobId, name) {
    clearInterval(pollRef.current); clearInterval(timerRef.current);
    const start = Date.now();
    timerRef.current = setInterval(() => setElapsed(Date.now() - start), 1000);
    pollRef.current = setInterval(async () => {
      try {
        const j = await api(`/jobs/${jobId}`);
        setJob(j);
        document.title = `[${Math.round(j.progress || 0)}%] ${name} — 23andClaude`;
        if (j.status === "complete") {
          clearInterval(pollRef.current); clearInterval(timerRef.current);
          setView("results");
          document.title = `✓ ${name} — 23andClaude`;
          if (Notification.permission === "granted") {
            new Notification("23andClaude Ancestry", { body: `${name} analysis complete!`, icon: "🧬" });
          }
          toast(`${name} analysis complete!`, "success");
        } else if (j.status === "failed") {
          clearInterval(pollRef.current); clearInterval(timerRef.current);
          document.title = `✗ ${name} — 23andClaude`;
          toast(`${name} analysis failed`, "error");
        }
      } catch {}
    }, 2000);
  }

  function viewJob(jobId) {
    api(`/jobs/${jobId}`).then((j) => {
      setJob(j); setTab("analyze"); setSampleName(j.sample_name || "");
      if (j.result) {
        setView("results");
      } else if (j.status === "failed") {
        setView("progress");
      } else {
        // Job is still running — start polling so progress updates live
        setView("progress");
        startPolling(jobId, j.sample_name || "Analysis");
      }
      window.location.hash = `results/${jobId}`;
    }).catch(() => {});
  }

  function goAnalyze() { setTab("analyze"); if (view !== "progress") resetForm(); }

  // Request notification permission
  useEffect(() => {
    if ("Notification" in window && Notification.permission === "default") {
      Notification.requestPermission();
    }
  }, []);

  return (
    <div style={s.page}>
      <div style={s.container}>
        {/* Header */}
        <div style={s.header}>
          <div style={s.headerLeft}>
            <span style={s.headerIcon}>🧬</span>
            <div>
              <h1 style={s.headerTitle}>23andClaude Ancestry</h1>
              <p style={s.headerSub}>Population composition from whole-genome data</p>
            </div>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <span style={{ fontSize: 13, color: "#8b949e" }}>{user.email}</span>
            <button
              onClick={onLogout}
              style={{
                background: "none", border: "1px solid #30363d", borderRadius: 6,
                color: "#8b949e", padding: "6px 12px", fontSize: 12, cursor: "pointer",
                fontFamily: "inherit",
              }}
            >
              Sign Out
            </button>
          </div>
        </div>

        {/* Tabs */}
        <div style={s.tabBar}>
          {[{ id: "home", label: "Overview" }, { id: "analyze", label: "Analyze" },
            { id: "compare", label: "Compare" }, { id: "history", label: "History" }].map((t) => (
            <button key={t.id}
              style={{ ...s.tab, ...(tab === t.id ? s.tabActive : {}) }}
              onClick={() => { setTab(t.id); if (t.id === "history") loadHistory(); if (t.id === "analyze" && view === "form") resetForm(); }}>
              {t.label}
            </button>
          ))}
        </div>

        {refReady === false && tab === "analyze" && (
          <div style={s.warning}>Reference panel not ready. Check the Overview tab for details.</div>
        )}

        {/* ── Overview ── */}
        {tab === "home" && (<>
          {/* Quick-run file selector — always visible on home tab */}
          <div style={{ ...s.card, marginBottom: 16, border: "1px solid #238636" }}>
            <div style={{ fontSize: 16, fontWeight: 600, color: "#e6edf3", marginBottom: 10 }}>
              🧬 Run Ancestry Analysis
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
              <select
                value={filePath}
                onChange={(e) => {
                  const path = e.target.value;
                  setFilePath(path);
                  if (path) {
                    const match = serverFiles.find((f) => f.path === path);
                    if (match) setSampleName(match.sample_name);
                    if (path.toLowerCase().endsWith(".bam") || path.toLowerCase().endsWith(".cram")) {
                      setFastaPath("/data/genom-nimo/reference.fasta");
                    } else {
                      setFastaPath("");
                    }
                    setInputMode("path");
                  }
                }}
                style={{
                  flex: 1, minWidth: 300, padding: "10px 14px", fontSize: 14,
                  background: "#0d1117", border: "1px solid #30363d", color: "#e6edf3",
                  borderRadius: 6, cursor: "pointer",
                }}>
                <option value="">Select a BAM / VCF / gVCF file...</option>
                {serverFiles.map((f) => (
                  <option key={f.path} value={f.path}>
                    {f.sample_name} [{f.name}] ({f.size_mb > 1000 ? `${(f.size_mb / 1024).toFixed(1)} GB` : `${Math.round(f.size_mb)} MB`})
                  </option>
                ))}
              </select>
              <button
                onClick={() => { if (filePath) { setTab("analyze"); setView("form"); setTimeout(() => handleSubmit(), 200); } }}
                disabled={!filePath || !sampleName.trim()}
                style={{
                  padding: "10px 24px", borderRadius: 6, fontSize: 14, fontWeight: 600,
                  border: "1px solid #238636", cursor: filePath ? "pointer" : "not-allowed",
                  background: filePath ? "#238636" : "#21262d",
                  color: filePath ? "#fff" : "#484f58",
                  whiteSpace: "nowrap",
                }}>
                Run Ancestry
              </button>
            </div>
            {serverFiles.length === 0 && (
              <div style={{ fontSize: 12, color: "#8b949e", marginTop: 8 }}>Loading server files...</div>
            )}
          </div>
          <OverviewTab refStatus={refStatus} refDetail={refDetail} onStartAnalysis={goAnalyze} history={history} viewJob={viewJob} />
        </>)}

        {/* ── Analyze ── */}
        {tab === "analyze" && (
          <div>
            {/* Form */}
            {view === "form" && (
              <div>
                <div style={s.card}>
                  <div style={{ fontSize: 16, fontWeight: 600, color: "#e6edf3", marginBottom: 6 }}>New Ancestry Analysis</div>
                  <p style={{ fontSize: 13, color: "#8b949e", margin: "0 0 20px", lineHeight: 1.5 }}>
                    Select a sample file to analyze. The pipeline will extract variants, merge with the
                    reference panel, and estimate ancestral composition across {refDetail?.stats?.group_count || 8} continental groups.
                  </p>

                  <div style={{ marginBottom: 20 }}>
                    <label style={s.label}>Sample Name</label>
                    <input style={s.input} value={sampleName} onChange={(e) => setSampleName(e.target.value)} placeholder="e.g., Nimo_WGS" />
                  </div>

                  <div style={{ marginBottom: 20 }}>
                    <label style={s.label}>Input Source</label>
                    <div style={s.toggle}>
                      <button style={{ ...s.toggleBtn, ...(inputMode === "upload" ? s.toggleActive : {}) }} onClick={() => setInputMode("upload")}>Upload File</button>
                      <button style={{ ...s.toggleBtn, ...(inputMode === "path" ? s.toggleActive : {}) }} onClick={() => setInputMode("path")}>Server File</button>
                    </div>

                    {inputMode === "upload" ? (
                      <div style={{ ...s.dropZone, ...(dragging ? s.dropZoneActive : {}), ...(file ? s.dropZoneFile : {}) }}
                        onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
                        onDragLeave={() => setDragging(false)}
                        onDrop={(e) => { e.preventDefault(); setDragging(false); if (e.dataTransfer.files[0]) setFile(e.dataTransfer.files[0]); }}
                        onClick={() => document.getElementById("file-input").click()}>
                        <input id="file-input" type="file" accept=".vcf,.vcf.gz,.g.vcf,.g.vcf.gz,.gvcf,.gvcf.gz" style={{ display: "none" }}
                          onChange={(e) => { if (e.target.files[0]) setFile(e.target.files[0]); }} />
                        {file ? (
                          <div>
                            <div style={{ fontSize: 24, marginBottom: 8 }}>✅</div>
                            <div style={{ color: "#3fb950", fontWeight: 600 }}>{file.name}</div>
                            <div style={{ fontSize: 12, marginTop: 4, color: "#8b949e" }}>{(file.size / 1e6).toFixed(1)} MB</div>
                          </div>
                        ) : (
                          <div>
                            <div style={{ fontSize: 24, marginBottom: 8 }}>📁</div>
                            <div>Drop VCF/gVCF file here or click to browse</div>
                            <div style={{ fontSize: 12, marginTop: 4 }}>Supports .vcf, .vcf.gz, .g.vcf.gz</div>
                          </div>
                        )}
                      </div>
                    ) : (
                      <div>
                        {serverFiles.length > 0 && (
                          <select style={s.select} value={filePath}
                            onChange={(e) => {
                              const path = e.target.value;
                              setFilePath(path);
                              if (path) {
                                const match = serverFiles.find((f) => f.path === path);
                                if (match && !sampleName.trim()) setSampleName(match.sample_name);
                                if (path.toLowerCase().endsWith(".bam") || path.toLowerCase().endsWith(".cram")) {
                                  if (!fastaPath.trim()) setFastaPath("/data/genom-nimo/reference.fasta");
                                }
                              }
                            }}>
                            <option value="">Select a file from the server...</option>
                            {serverFiles.map((f) => (
                              <option key={f.path} value={f.path}>
                                {f.name} ({f.size_mb > 1000 ? `${(f.size_mb / 1024).toFixed(1)} GB` : `${f.size_mb} MB`})
                              </option>
                            ))}
                          </select>
                        )}
                        <input style={{ ...s.input, marginTop: serverFiles.length > 0 ? 8 : 0 }} value={filePath}
                          onChange={(e) => setFilePath(e.target.value)}
                          placeholder={serverFiles.length > 0 ? "Or type a custom path..." : "/data/aligned_bams/sample.bam"} />
                        {showFasta && (
                          <div style={{ marginTop: 12 }}>
                            <label style={s.label}>Reference FASTA (required for BAM/CRAM)</label>
                            <input style={s.input} value={fastaPath} onChange={(e) => setFastaPath(e.target.value)} placeholder="/data/refs/GRCh38.fa" />
                          </div>
                        )}
                      </div>
                    )}
                  </div>

                  <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
                    <button style={{ ...s.btn, ...s.btnPrimary, ...(submitting || refReady === false ? s.btnDisabled : {}) }}
                      disabled={submitting || refReady === false} onClick={handleSubmit}>
                      {submitting ? "Starting..." : "Analyze Ancestry"}
                    </button>
                  </div>
                  {error && <div style={s.error}>{error}</div>}
                </div>

                {/* Batch Analyze */}
                {serverFiles.length > 1 && (
                  <BatchAnalyze serverFiles={serverFiles} onQueued={() => { loadHistory(); setTab("history"); }} />
                )}

                <div style={s.infoBox}>
                  <strong style={{ color: "#e6edf3" }}>Supported formats:</strong><br />
                  <strong>VCF / gVCF</strong> — Standard variant call format. Compressed (.vcf.gz) preferred.<br />
                  <strong>BAM / CRAM</strong> — Aligned reads. Requires a reference FASTA path. Variants called via bcftools mpileup.<br /><br />
                  <strong style={{ color: "#e6edf3" }}>Requirements:</strong> GRCh38/hg38 coordinates. Whole-genome or whole-exome data.
                  Minimum ~50,000 overlapping variants with the reference panel.
                </div>
              </div>
            )}

            {/* Progress */}
            {view === "progress" && job && (
              <div style={s.card}>
                <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 20 }}>
                  <div>
                    <div style={{ fontSize: 18, fontWeight: 600, color: "#e6edf3" }}>{sampleName || job.sample_name || "Analysis"}</div>
                    <div style={{ fontSize: 12, color: "#8b949e", marginTop: 2 }}>
                      {job.status === "failed" ? "Failed" : "Running ancestry inference..."}
                    </div>
                  </div>
                  <div style={{ fontSize: 13, color: "#8b949e" }}>{Math.floor(elapsed / 1000)}s</div>
                </div>
                <div style={{ marginBottom: 16 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
                    <span style={{ fontSize: 13, color: "#8b949e" }}>{job.current_step || "Queued..."}</span>
                    <span style={{ fontSize: 13, fontWeight: 600, color: "#e6edf3" }}>{Math.round(job.progress || 0)}%</span>
                  </div>
                  <div style={s.progressTrack}>
                    <div style={{ ...s.progressFill, width: `${job.progress || 0}%` }} />
                  </div>
                </div>
                {job.status === "failed" && (
                  <div>
                    <div style={s.error}>{job.error || "Pipeline failed"}</div>
                    <div style={{ marginTop: 16 }}><button style={{ ...s.btn, ...s.btnPrimary }} onClick={resetForm}>Try Again</button></div>
                  </div>
                )}
                {job.status !== "failed" && (
                  <button style={{ ...s.btn, ...s.btnSecondary, marginTop: 8 }}
                    onClick={() => { clearInterval(pollRef.current); clearInterval(timerRef.current); resetForm(); }}>Cancel</button>
                )}
              </div>
            )}

            {/* Results */}
            {view === "results" && job?.result && (
              <div>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
                  <div>
                    <div style={{ fontSize: 20, fontWeight: 600, color: "#e6edf3" }}>{job.result.sample_name}</div>
                    <div style={{ fontSize: 13, color: "#8b949e" }}>
                      Primary: {job.result.primary} ({fmtPct(job.result.primary_pct)}%)
                      {job.result.is_admixed && " · Admixed"}
                    </div>
                  </div>
                  <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                    <button style={{ ...s.btn, ...s.btnSecondary, padding: "8px 14px", fontSize: 13 }} onClick={() => {
                      const url = `${window.location.origin}/ancestry/#results/${job.job_id}`;
                      navigator.clipboard.writeText(url).then(() => toast("Link copied!", "success"));
                    }} title="Copy shareable link">🔗 Link</button>
                    <button style={{ ...s.btn, ...s.btnSecondary, padding: "8px 14px", fontSize: 13 }} onClick={exportPNG}>PNG</button>
                    <button style={{ ...s.btn, ...s.btnSecondary, padding: "8px 14px", fontSize: 13 }}
                      onClick={() => window.open(`/api/jobs/${job.job_id}/csv`, "_blank")}>CSV</button>
                    <button style={{ ...s.btn, ...s.btnSecondary, padding: "8px 14px", fontSize: 13 }} onClick={downloadResult}>JSON</button>
                    <button style={{ ...s.btn, ...s.btnPrimary, padding: "8px 14px", fontSize: 13 }} onClick={resetForm}>New Analysis</button>
                  </div>
                </div>

                <div style={s.card}>
                  <div style={{ ...s.sectionTitle, fontSize: 16, marginTop: 0 }}>Ancestry Composition</div>
                  <CompositionChart proportions={job.result.proportions} />
                </div>

                <SignaturesSection signatures={job.result.signatures} />
                <WorldMap proportions={job.result.proportions} />
                <PCAPlot pca={job.result.pca} sampleName={job.result.sample_name} />
                <AncestryContext proportions={job.result.proportions} />
                <PopulationBreakdown popProportions={job.result.pop_proportions} proportions={job.result.proportions} />
                <Flags flags={job.result.flags} />
                <ROH roh={job.result.roh} />
                <TechDetails result={job.result} job={job} />
              </div>
            )}

            {view === "results" && job && !job.result && (
              <div style={s.card}>
                <div style={{ fontSize: 18, fontWeight: 600, color: "#f85149", marginBottom: 12 }}>Analysis Failed</div>
                <div style={s.error}>{job.error || "Unknown error"}</div>
                <button style={{ ...s.btn, ...s.btnPrimary, marginTop: 16 }} onClick={resetForm}>Try Again</button>
              </div>
            )}
          </div>
        )}

        {/* ── Compare ── */}
        {tab === "compare" && <CompareTab history={history} loadHistory={loadHistory} />}

        {/* ── History ── */}
        {tab === "history" && <HistoryTab history={history} loadHistory={loadHistory} viewJob={viewJob} goAnalyze={goAnalyze} />}

        {/* Keyboard shortcut hint */}
        <div style={{ textAlign: "center", padding: "24px 0 0", fontSize: 12, color: "#30363d" }}>
          Press <kbd style={kbdStyle}>?</kbd> for keyboard shortcuts
        </div>
      </div>

      {/* Shortcuts overlay */}
      {showShortcuts && (
        <div style={overlayStyle} onClick={() => setShowShortcuts(false)}>
          <div style={overlayCardStyle} onClick={(e) => e.stopPropagation()}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
              <div style={{ fontSize: 16, fontWeight: 600, color: "#e6edf3" }}>Keyboard Shortcuts</div>
              <button style={{ background: "none", border: "none", color: "#8b949e", fontSize: 18, cursor: "pointer" }}
                onClick={() => setShowShortcuts(false)}>✕</button>
            </div>
            {[
              ["1", "Overview tab"],
              ["2", "Analyze tab"],
              ["3", "Compare tab"],
              ["4", "History tab"],
              ["N", "New analysis"],
              ["R", "Refresh history"],
              ["?", "Toggle this help"],
              ["Esc", "Close overlay"],
            ].map(([key, desc]) => (
              <div key={key} style={{ display: "flex", justifyContent: "space-between", padding: "6px 0", borderBottom: "1px solid #21262d" }}>
                <span style={{ color: "#8b949e", fontSize: 13 }}>{desc}</span>
                <kbd style={kbdStyle}>{key}</kbd>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Toast notifications */}
      <ToastContainer toasts={toasts} />

      {/* Inject responsive CSS */}
      <style>{responsiveCSS}</style>
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════
   Styles
   ═══════════════════════════════════════════════════════════════ */
const s = {
  page: { minHeight: "100vh", background: "#0d1117", padding: "0 16px" },
  container: { maxWidth: 800, margin: "0 auto", padding: "24px 0 64px" },
  header: { display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24 },
  headerLeft: { display: "flex", alignItems: "center", gap: 14 },
  headerIcon: { fontSize: 36 },
  headerTitle: { margin: 0, fontSize: 22, fontWeight: 700, color: "#e6edf3" },
  headerSub: { margin: "2px 0 0", fontSize: 13, color: "#8b949e" },
  backLink: { color: "#58a6ff", textDecoration: "none", fontSize: 13 },
  tabBar: { display: "flex", gap: 4, marginBottom: 24, borderBottom: "1px solid #21262d" },
  tab: { background: "none", border: "none", borderBottom: "2px solid transparent", color: "#8b949e", padding: "10px 16px", fontSize: 14, cursor: "pointer", fontFamily: "inherit" },
  tabActive: { color: "#e6edf3", borderBottomColor: "#58a6ff" },
  card: { background: "#161b22", border: "1px solid #30363d", borderRadius: 10, padding: 24, marginBottom: 20 },
  label: { display: "block", fontSize: 13, fontWeight: 500, color: "#c9d1d9", marginBottom: 6 },
  input: { width: "100%", padding: "10px 14px", background: "#0d1117", border: "1px solid #30363d", borderRadius: 6, color: "#c9d1d9", fontSize: 14, fontFamily: "inherit", boxSizing: "border-box", outline: "none" },
  select: { width: "100%", padding: "10px 14px", background: "#0d1117", border: "1px solid #30363d", borderRadius: 6, color: "#c9d1d9", fontSize: 14, fontFamily: "inherit", boxSizing: "border-box", outline: "none", cursor: "pointer" },
  toggle: { display: "flex", gap: 0, marginBottom: 16, borderRadius: 6, overflow: "hidden", border: "1px solid #30363d", width: "fit-content" },
  toggleBtn: { background: "#0d1117", border: "none", color: "#8b949e", padding: "8px 20px", fontSize: 13, cursor: "pointer", fontFamily: "inherit" },
  toggleActive: { background: "#21262d", color: "#e6edf3" },
  dropZone: { border: "2px dashed #30363d", borderRadius: 8, padding: "32px 20px", textAlign: "center", color: "#8b949e", cursor: "pointer", fontSize: 14 },
  dropZoneActive: { borderColor: "#58a6ff" },
  dropZoneFile: { borderColor: "#3fb950", borderStyle: "solid" },
  btn: { padding: "10px 24px", borderRadius: 6, border: "none", fontSize: 14, fontWeight: 500, cursor: "pointer", fontFamily: "inherit" },
  btnPrimary: { background: "#238636", color: "#fff" },
  btnSecondary: { background: "#21262d", color: "#c9d1d9", border: "1px solid #30363d" },
  btnDisabled: { opacity: 0.5, cursor: "not-allowed" },
  error: { background: "#f8514922", border: "1px solid #f8514944", borderRadius: 6, padding: "10px 14px", color: "#f85149", fontSize: 13, marginTop: 12, lineHeight: 1.5, whiteSpace: "pre-wrap" },
  warning: { background: "#d2992222", border: "1px solid #d2992244", borderRadius: 6, padding: "10px 14px", color: "#d29922", fontSize: 13, marginBottom: 20, lineHeight: 1.5 },
  infoBox: { background: "#161b22", border: "1px solid #21262d", borderRadius: 8, padding: 20, marginTop: 16, fontSize: 13, color: "#8b949e", lineHeight: 1.7 },
  sectionTitle: { fontSize: 16, fontWeight: 600, color: "#e6edf3", marginBottom: 12, marginTop: 24 },
  progressTrack: { height: 8, background: "#21262d", borderRadius: 4, overflow: "hidden" },
  progressFill: { height: "100%", background: "linear-gradient(90deg, #238636, #3fb950)", borderRadius: 4, transition: "width 0.3s ease" },
  compBar: { display: "flex", height: 32, borderRadius: 6, overflow: "hidden", marginBottom: 16 },
  compSegment: { display: "flex", alignItems: "center", justifyContent: "center", fontSize: 11, fontWeight: 600, color: "#fff", minWidth: 0, transition: "width 0.3s" },
  compGrid: { display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(120px, 1fr))", gap: 10 },
  compCard: { display: "flex", alignItems: "center", gap: 10, padding: "8px 12px", background: "#0d1117", borderRadius: 6, border: "1px solid #21262d" },
  compDot: { width: 12, height: 12, borderRadius: "50%", flexShrink: 0 },
  compPct: { fontSize: 15, fontWeight: 700, color: "#e6edf3" },
  compLabel: { fontSize: 11, color: "#8b949e" },
  flagBox: { display: "flex", gap: 10, alignItems: "flex-start", background: "#161b22", border: "1px solid #30363d", borderRadius: 8, padding: "12px 16px", marginBottom: 10 },
  rohCard: { display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(120px, 1fr))", gap: 16, background: "#161b22", border: "1px solid #30363d", borderRadius: 8, padding: 20 },
  rohVal: { fontSize: 20, fontWeight: 700, color: "#e6edf3" },
  rohLabel: { fontSize: 11, color: "#8b949e", marginTop: 2 },
  techRow: { display: "flex", justifyContent: "space-between", padding: "8px 0", borderBottom: "1px solid #21262d", fontSize: 13, color: "#e6edf3" },
  statBox: { background: "#0d1117", borderRadius: 8, padding: 14, border: "1px solid #21262d", textAlign: "center" },
  historyRow: { display: "flex", justifyContent: "space-between", alignItems: "center", padding: "14px 16px", background: "#161b22", border: "1px solid #21262d", borderRadius: 8, marginBottom: 8, cursor: "pointer" },
};

const kbdStyle = {
  display: "inline-block", padding: "2px 6px", background: "#21262d", border: "1px solid #30363d",
  borderRadius: 4, fontSize: 12, fontFamily: "monospace", color: "#c9d1d9", lineHeight: 1.4,
};

const overlayStyle = {
  position: "fixed", top: 0, left: 0, right: 0, bottom: 0,
  background: "rgba(0,0,0,0.6)", display: "flex", alignItems: "center",
  justifyContent: "center", zIndex: 1000,
};

const overlayCardStyle = {
  background: "#161b22", border: "1px solid #30363d", borderRadius: 12,
  padding: "24px 32px", width: 360, maxWidth: "90vw",
};

const responsiveCSS = `
  @keyframes fadeIn { from { opacity: 0; transform: translateX(20px); } to { opacity: 1; transform: translateX(0); } }
  @media (max-width: 640px) {
    canvas { max-width: 100% !important; height: auto !important; }
    h1 { font-size: 18px !important; }
    table { font-size: 12px !important; }
    th, td { padding: 6px 8px !important; }
  }
  @media print {
    body { background: #fff !important; color: #000 !important; }
    button, a[href="/"], [style*="tabBar"], [style*="backLink"] { display: none !important; }
    div[style*="161b22"] { background: #f8f8f8 !important; border-color: #ddd !important; }
    div[style*="0d1117"] { background: #fff !important; }
    * { color: #000 !important; border-color: #ccc !important; }
    canvas { max-width: 100% !important; }
  }
`;
