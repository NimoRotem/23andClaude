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
    ctx.translate(14, H / 2);
    ctx.rotate(-Math.PI / 2);
    ctx.fillText(pcLabels[axY], 0, 0);
    ctx.restore();
    ctx.textAlign = "start";

    // Legend
    const legendX = W - 130, legendY = 16;
    const groups = Object.keys(GROUP_COLORS);
    groups.forEach((g, i) => {
      ctx.fillStyle = groupColor(g);
      ctx.beginPath();
      ctx.arc(legendX, legendY + i * 18, 4, 0, Math.PI * 2);
      ctx.fill();
      ctx.fillStyle = "#8b949e";
      ctx.font = "10px -apple-system, sans-serif";
      ctx.fillText(g, legendX + 10, legendY + i * 18 + 3);
    });
  }, [pca, axes, sampleName, extraQueries]);

  if (!pca?.query) return null;

  return (
    <div>
      <div style={{ ...s.sectionTitle, fontSize: 16, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <span>PCA Plot</span>
        <div style={{ display: "flex", gap: 6 }}>
          <button
            style={{ ...s.btn, ...s.btnSecondary, padding: "4px 10px", fontSize: 11, ...(axes[0] === 0 ? { background: "#30363d" } : {}) }}
            onClick={() => setAxes([0, 1])}
          >
            PC1 vs PC2
          </button>
          <button
            style={{ ...s.btn, ...s.btnSecondary, padding: "4px 10px", fontSize: 11, ...(axes[0] === 2 ? { background: "#30363d" } : {}) }}
            onClick={() => setAxes([2, 3])}
          >
            PC3 vs PC4
          </button>
        </div>
      </div>
      <div style={{ ...s.card, padding: 0, overflow: "hidden" }}>
        <canvas
          ref={canvasRef}
          width={760}
          height={440}
          style={{ width: "100%", height: "auto", display: "block" }}
        />
      </div>
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════
   Population Breakdown
   ═══════════════════════════════════════════════════════════════ */
function PopulationBreakdown({ popProportions, proportions }) {
  const [open, setOpen] = useState(false);
  if (!popProportions || Object.keys(popProportions).length === 0) return null;

  // Group populations by their continental group
  const grouped = {};
  for (const [pop, val] of Object.entries(popProportions)) {
    const group = POP_TO_GROUP[pop] || "Other";
    if (!grouped[group]) grouped[group] = [];
    grouped[group].push([pop, val]);
  }
  // Sort groups by group-level proportion, pops within by value
  const sortedGroups = Object.entries(grouped)
    .sort((a, b) => {
      const aSum = a[1].reduce((s, [, v]) => s + v, 0);
      const bSum = b[1].reduce((s, [, v]) => s + v, 0);
      return bSum - aSum;
    });
  for (const [, pops] of sortedGroups) {
    pops.sort((a, b) => b[1] - a[1]);
  }

  return (
    <div>
      <div
        style={{ ...s.sectionTitle, fontSize: 16, cursor: "pointer", display: "flex", alignItems: "center", gap: 8 }}
        onClick={() => setOpen(!open)}
      >
        Population-Level Detail
        <span style={{ fontSize: 12, color: "#8b949e" }}>{open ? "▲" : "▼"}</span>
        <span style={{ fontSize: 12, color: "#8b949e", fontWeight: 400 }}>
          ({Object.keys(popProportions).length} populations detected)
        </span>
      </div>
      {open && (
        <div style={s.card}>
          {sortedGroups.map(([group, pops]) => (
            <div key={group} style={{ marginBottom: 16 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
                <div style={{ width: 10, height: 10, borderRadius: "50%", background: groupColor(group), flexShrink: 0 }} />
                <span style={{ fontSize: 14, fontWeight: 600, color: "#e6edf3" }}>{group}</span>
                <span style={{ fontSize: 12, color: "#8b949e" }}>
                  ({(pops.reduce((s, [, v]) => s + v, 0) * 100).toFixed(1)}%)
                </span>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(180px, 1fr))", gap: 6, marginLeft: 18 }}>
                {pops.map(([pop, val]) => (
                  <div key={pop} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "4px 10px", background: "#0d1117", borderRadius: 4, border: "1px solid #21262d" }}>
                    <span style={{ fontSize: 12, color: "#c9d1d9" }}>{pop}</span>
                    <span style={{ fontSize: 12, fontWeight: 600, color: groupColor(group) }}>
                      {(val * 100).toFixed(1)}%
                    </span>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════
   Composition, Flags, ROH, TechDetails (existing)
   ═══════════════════════════════════════════════════════════════ */
function CompositionChart({ proportions }) {
  const sorted = Object.entries(proportions).sort((a, b) => b[1] - a[1]);
  return (
    <>
      <div style={s.compBar}>
        {sorted.map(([g, v]) => (
          <div key={g} title={`${g}: ${(v * 100).toFixed(1)}%`}
            style={{ ...s.compSegment, width: `${Math.max(v * 100, 0.5)}%`, background: groupColor(g) }}>
            {v > 0.05 ? `${(v * 100).toFixed(0)}%` : ""}
          </div>
        ))}
      </div>
      <div style={s.compGrid}>
        {sorted.filter(([, v]) => v > 0.005).map(([g, v]) => (
          <div key={g} style={s.compCard}>
            <div style={{ ...s.compDot, background: groupColor(g) }} />
            <div>
              <div style={s.compPct}>{(v * 100).toFixed(1)}%</div>
              <div style={s.compLabel}>{g}</div>
            </div>
          </div>
        ))}
      </div>
    </>
  );
}

function SignatureCard({ match }) {
  const isHigh = match.confidence >= 0.7;
  const borderColor = isHigh ? "#3fb950" : "#d29922";
  const badgeBg = isHigh ? "#0d3117" : "#2d2000";
  const badgeColor = isHigh ? "#3fb950" : "#d29922";
  const confLabel = isHigh ? "High" : "Moderate";
  const confPct = Math.round(match.confidence * 100);

  return (
    <div style={{
      background: "#161b22", border: `1px solid ${borderColor}`, borderLeft: `4px solid ${borderColor}`,
      borderRadius: 10, padding: "20px 24px", marginBottom: 12,
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 10 }}>
        <div style={{ fontSize: 17, fontWeight: 700, color: "#e6edf3" }}>{match.display_name}</div>
        <span style={{
          padding: "3px 10px", borderRadius: 12, fontSize: 12, fontWeight: 600,
          background: badgeBg, color: badgeColor, whiteSpace: "nowrap",
        }}>
          {confLabel} ({confPct}%)
        </span>
      </div>
      {match.description && (
        <div style={{ fontSize: 13, color: "#8b949e", lineHeight: 1.5, marginBottom: 14 }}>
          {match.description}
        </div>
      )}
      {match.details && match.details.length > 0 && (
        <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
          {match.details.map((d, i) => (
            <div key={i} style={{
              background: "#0d1117", border: "1px solid #30363d", borderRadius: 6,
              padding: "6px 12px", display: "flex", alignItems: "center", gap: 8, fontSize: 12,
            }}>
              <span style={{ color: "#e6edf3", fontWeight: 600 }}>{d.group}</span>
              <span style={{ color: d.in_range ? "#3fb950" : "#f85149", fontWeight: 700 }}>
                {d.actual}%
              </span>
              <span style={{ color: "#6e7681" }}>
                [{d.range[0]}-{d.range[1]}%]
              </span>
              <span style={{ fontSize: 10 }}>
                {d.in_range ? "\u2713" : "\u2717"}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function SignaturesSection({ signatures }) {
  const [showPartial, setShowPartial] = useState(false);

  if (!signatures || signatures.length === 0) return null;

  const full = signatures.filter((sig) => sig.confidence >= 0.3);
  const partial = signatures.filter((sig) => sig.confidence > 0 && sig.confidence < 0.3);

  if (full.length === 0 && partial.length === 0) return null;

  return (
    <div>
      <div style={{ ...s.sectionTitle, fontSize: 16 }}>Population Signatures</div>
      {full.map((m, i) => <SignatureCard key={m.id || i} match={m} />)}
      {partial.length > 0 && (
        <div>
          <div
            style={{ fontSize: 13, color: "#8b949e", cursor: "pointer", marginTop: 8, marginBottom: 8, display: "flex", alignItems: "center", gap: 6 }}
            onClick={() => setShowPartial(!showPartial)}
          >
            {partial.length} partial match{partial.length > 1 ? "es" : ""}
            <span style={{ fontSize: 10 }}>{showPartial ? "\u25B2" : "\u25BC"}</span>
          </div>
          {showPartial && partial.map((m, i) => <SignatureCard key={m.id || i} match={m} />)}
        </div>
      )}
    </div>
  );
}

function Flags({ flags }) {
  if (!flags || !flags.length) return null;
  return (
    <div>
      <div style={{ ...s.sectionTitle, fontSize: 16 }}>Interpretation</div>
      {flags.map((f, i) => (
        <div key={i} style={s.flagBox}>
          <span style={{ fontSize: 18, flexShrink: 0 }}>🔍</span>
          <span style={{ fontSize: 13, lineHeight: 1.5 }}>{f}</span>
        </div>
      ))}
    </div>
  );
}

function AncestryContext({ proportions }) {
  const significant = Object.entries(proportions)
    .filter(([, v]) => v > 0.02)
    .sort((a, b) => b[1] - a[1]);
  if (significant.length === 0) return null;

  return (
    <div>
      <div style={{ ...s.sectionTitle, fontSize: 16 }}>Ancestry Context</div>
      {significant.map(([group, val]) => {
        const info = GROUP_INFO[group];
        if (!info) return null;
        return (
          <div key={group} style={{
            background: "#161b22", border: "1px solid #30363d", borderRadius: 8,
            padding: "14px 18px", marginBottom: 10, display: "flex", gap: 14, alignItems: "flex-start",
          }}>
            <span style={{ fontSize: 28, flexShrink: 0, lineHeight: 1 }}>{info.emoji}</span>
            <div style={{ flex: 1 }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 4 }}>
                <span style={{ fontSize: 14, fontWeight: 600, color: groupColor(group) }}>{group}</span>
                <span style={{ fontSize: 13, fontWeight: 700, color: "#e6edf3" }}>{(val * 100).toFixed(1)}%</span>
              </div>
              <div style={{ fontSize: 11, color: "#58a6ff", marginBottom: 4 }}>{info.region}</div>
              <div style={{ fontSize: 12, color: "#8b949e", lineHeight: 1.6 }}>{info.desc}</div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

function WorldMap({ proportions }) {
  const canvasRef = useRef(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    const W = canvas.width, H = canvas.height;

    ctx.clearRect(0, 0, W, H);
    ctx.fillStyle = "#0d1117";
    ctx.fillRect(0, 0, W, H);

    // Simple equirectangular projection: lon [-180,180] -> [0,W], lat [-60,80] -> [H,0]
    const lonToX = (lon) => ((lon + 180) / 360) * W;
    const latToY = (lat) => ((80 - lat) / 140) * H;

    // Draw simplified continent outlines (coarse polygons)
    const continents = [
      // Europe
      [[-10,36],[-10,60],[0,62],[10,65],[30,70],[40,65],[45,55],[40,40],[25,35],[10,36]],
      // Africa
      [[-18,15],[-18,35],[-5,36],[10,36],[15,30],[35,30],[42,12],[50,5],[42,-5],[35,-25],[28,-35],[18,-35],[10,-20],[5,5],[-5,5],[-18,10]],
      // Asia
      [[30,70],[40,65],[45,55],[50,50],[60,55],[70,55],[80,50],[90,50],[100,55],[110,55],[120,55],[130,50],[140,45],[145,50],[150,60],[160,65],[170,65],[180,65],[180,25],[140,20],[120,10],[105,10],[100,20],[95,20],[80,25],[70,25],[60,30],[50,40],[40,40],[35,35],[30,35],[30,70]],
      // Americas
      [[-130,55],[-125,60],[-140,70],[-170,65],[-165,60],[-140,55],[-125,50],[-120,35],[-115,30],[-105,20],[-100,20],[-85,10],[-80,10],[-75,5],[-80,0],[-75,-10],[-60,-5],[-50,-5],[-45,-15],[-40,-20],[-50,-30],[-55,-40],[-70,-55],[-75,-50],[-75,-40],[-70,-18],[-80,-5],[-80,5],[-85,12],[-90,15],[-95,20],[-105,25],[-120,35],[-125,48]],
      // Australia
      [[115,-12],[130,-12],[140,-12],[150,-18],[153,-25],[150,-35],[140,-38],[130,-35],[120,-25],[115,-20]],
    ];

    ctx.strokeStyle = "#21262d";
    ctx.lineWidth = 1;
    for (const poly of continents) {
      ctx.beginPath();
      for (let i = 0; i < poly.length; i++) {
        const x = lonToX(poly[i][0]), y = latToY(poly[i][1]);
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
      }
      ctx.closePath();
      ctx.fillStyle = "#161b2288";
      ctx.fill();
      ctx.stroke();
    }

    // Draw ancestry bubbles
    const significant = Object.entries(proportions).filter(([, v]) => v > 0.01).sort((a, b) => b[1] - a[1]);
    for (const [group, val] of significant) {
      const geo = GROUP_GEO[group];
      if (!geo) continue;
      const x = lonToX(geo[0]), y = latToY(geo[1]);
      const radius = Math.max(8, Math.sqrt(val) * 50);

      // Glow
      ctx.globalAlpha = 0.2;
      ctx.fillStyle = groupColor(group);
      ctx.beginPath(); ctx.arc(x, y, radius + 8, 0, Math.PI * 2); ctx.fill();

      // Solid circle
      ctx.globalAlpha = 0.7;
      ctx.beginPath(); ctx.arc(x, y, radius, 0, Math.PI * 2); ctx.fill();

      // Percentage label
      ctx.globalAlpha = 1;
      ctx.fillStyle = "#fff";
      ctx.font = `bold ${Math.max(11, radius * 0.5)}px -apple-system, sans-serif`;
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      const pctText = `${(val * 100).toFixed(0)}%`;
      ctx.fillText(pctText, x, y);

      // Group name below
      ctx.fillStyle = "#c9d1d9";
      ctx.font = "10px -apple-system, sans-serif";
      ctx.fillText(group, x, y + radius + 14);
    }
    ctx.textAlign = "start";
    ctx.textBaseline = "alphabetic";
    ctx.globalAlpha = 1;
  }, [proportions]);

  return (
    <div>
      <div style={{ ...s.sectionTitle, fontSize: 16 }}>Ancestry Map</div>
      <div style={{ ...s.card, padding: 0, overflow: "hidden" }}>
        <canvas ref={canvasRef} width={760} height={380}
          style={{ width: "100%", height: "auto", display: "block" }} />
      </div>
    </div>
  );
}

function ROH({ roh }) {
  if (!roh) return null;
  return (
    <div>
      <div style={{ ...s.sectionTitle, fontSize: 16 }}>Runs of Homozygosity (ROH)</div>
      <div style={s.rohCard}>
        <div><div style={s.rohVal}>{roh.total_mb?.toFixed(1) || "0"}</div><div style={s.rohLabel}>Total ROH (Mb)</div></div>
        <div><div style={s.rohVal}>{roh.n_segments || 0}</div><div style={s.rohLabel}>Segments</div></div>
        <div><div style={s.rohVal}>{roh.avg_kb?.toFixed(0) || "0"}</div><div style={s.rohLabel}>Avg Segment (kb)</div></div>
        <div><div style={{ ...s.rohVal, color: roh.bottleneck ? "#d29922" : "#3fb950" }}>{roh.bottleneck ? "Yes" : "No"}</div><div style={s.rohLabel}>Bottleneck</div></div>
      </div>
      {roh.bottleneck && (
        <div style={{ ...s.flagBox, marginTop: 10 }}>
          <span style={{ fontSize: 18 }}>⚠️</span>
          <span style={{ fontSize: 13, lineHeight: 1.5 }}>
            Elevated ROH ({roh.total_mb?.toFixed(0)} Mb) indicates a population bottleneck in recent ancestry.
            This is common in Ashkenazi Jewish, Finnish, and certain island populations.
          </span>
        </div>
      )}
    </div>
  );
}

function TechDetails({ result, job }) {
  const [open, setOpen] = useState(false);
  return (
    <div>
      <div style={{ ...s.sectionTitle, fontSize: 16, cursor: "pointer", display: "flex", alignItems: "center", gap: 8 }}
        onClick={() => setOpen(!open)}>
        Technical Details
        <span style={{ fontSize: 12, color: "#8b949e" }}>{open ? "▲" : "▼"}</span>
      </div>
      {open && (
        <div style={{ ...s.card, padding: "12px 20px" }}>
          <div style={s.techRow}><span style={{ color: "#8b949e" }}>Variants used</span><span>{result.variants_used?.toLocaleString()}</span></div>
          <div style={s.techRow}><span style={{ color: "#8b949e" }}>Reference panel</span><span>{result.panel}</span></div>
          <div style={s.techRow}><span style={{ color: "#8b949e" }}>Primary ancestry</span>
            <span>{result.primary} ({fmtPct(result.primary_pct)}%)</span>
          </div>
          <div style={s.techRow}><span style={{ color: "#8b949e" }}>Admixed</span><span>{result.is_admixed ? "Yes" : "No"}</span></div>
          {job && (
            <div style={{ ...s.techRow, borderBottom: "none" }}>
              <span style={{ color: "#8b949e" }}>Duration</span>
              <span>{job.completed_at && job.created_at ? `${Math.round((new Date(job.completed_at) - new Date(job.created_at)) / 1000)}s` : "--"}</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════
   Comparison View
   ═══════════════════════════════════════════════════════════════ */
function CompareTab({ history, loadHistory }) {
  const [selected, setSelected] = useState(new Set());
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => { loadHistory(); }, []);

  const completedJobs = history.filter((h) => h.status === "complete" && h.has_result);

  function toggle(id) {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  }

  async function compare() {
    if (selected.size < 2) return;
    setLoading(true); setError(null); setResults(null);
    try {
      const data = await api(`/jobs/compare?ids=${[...selected].join(",")}`);
      setResults(data.comparisons);
    } catch (e) { setError(e.message); }
    finally { setLoading(false); }
  }

  // All groups across all results
  const allGroups = results
    ? [...new Set(results.flatMap((r) => Object.keys(r.proportions)))]
        .sort((a, b) => {
          const maxA = Math.max(...results.map((r) => r.proportions[a] || 0));
          const maxB = Math.max(...results.map((r) => r.proportions[b] || 0));
          return maxB - maxA;
        })
    : [];

  return (
    <div>
      <div style={{ ...s.sectionTitle, margin: "0 0 16px" }}>Compare Samples</div>

      {completedJobs.length < 2 && (
        <div style={{ color: "#8b949e", textAlign: "center", padding: 40 }}>
          Need at least 2 completed analyses to compare. Run more analyses first.
        </div>
      )}

      {completedJobs.length >= 2 && !results && (
        <div style={s.card}>
          <p style={{ fontSize: 13, color: "#8b949e", margin: "0 0 16px" }}>
            Select 2 or more completed analyses to compare side by side.
          </p>
          {completedJobs.map((h) => (
            <label key={h.job_id} style={{
              display: "flex", alignItems: "center", gap: 10, padding: "10px 12px",
              background: selected.has(h.job_id) ? "#21262d" : "#0d1117",
              border: `1px solid ${selected.has(h.job_id) ? "#58a6ff" : "#21262d"}`,
              borderRadius: 6, marginBottom: 6, cursor: "pointer",
            }}>
              <input type="checkbox" checked={selected.has(h.job_id)} onChange={() => toggle(h.job_id)}
                style={{ accentColor: "#58a6ff" }} />
              <div style={{ flex: 1 }}>
                <div style={{ fontSize: 14, fontWeight: 500, color: "#e6edf3" }}>{h.sample_name}</div>
                <div style={{ fontSize: 12, color: "#8b949e" }}>
                  {h.result_summary?.primary} ({fmtPct(h.result_summary?.primary_pct || 0)}%)
                  {" · "}{timeAgo(h.created_at)}
                </div>
              </div>
            </label>
          ))}
          <div style={{ marginTop: 16, display: "flex", gap: 12, alignItems: "center" }}>
            <button style={{ ...s.btn, ...s.btnPrimary, ...(selected.size < 2 || loading ? s.btnDisabled : {}) }}
              disabled={selected.size < 2 || loading} onClick={compare}>
              {loading ? "Loading..." : `Compare ${selected.size} Samples`}
            </button>
            {error && <span style={{ color: "#f85149", fontSize: 13 }}>{error}</span>}
          </div>
        </div>
      )}

      {results && (
        <div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
            <span style={{ fontSize: 14, color: "#8b949e" }}>{results.length} samples compared</span>
            <div style={{ display: "flex", gap: 8 }}>
              <button style={{ ...s.btn, ...s.btnSecondary, padding: "6px 14px", fontSize: 13 }}
                onClick={() => window.open("/api/export/all-csv", "_blank")}>
                Export CSV
              </button>
              <button style={{ ...s.btn, ...s.btnSecondary, padding: "6px 14px", fontSize: 13 }}
                onClick={() => { setResults(null); setSelected(new Set()); }}>
                New Comparison
              </button>
            </div>
          </div>

          {/* Stacked composition bars */}
          <div style={s.card}>
            <div style={{ ...s.sectionTitle, fontSize: 16, marginTop: 0 }}>Ancestry Composition</div>
            {results.map((r) => (
              <div key={r.job_id} style={{ marginBottom: 16 }}>
                <div style={{ fontSize: 13, fontWeight: 600, color: "#e6edf3", marginBottom: 6 }}>{r.sample_name}</div>
                <div style={s.compBar}>
                  {Object.entries(r.proportions).sort((a, b) => b[1] - a[1]).map(([g, v]) => (
                    <div key={g} title={`${g}: ${(v * 100).toFixed(1)}%`}
                      style={{ ...s.compSegment, width: `${Math.max(v * 100, 0.5)}%`, background: groupColor(g) }}>
                      {v > 0.08 ? `${(v * 100).toFixed(0)}%` : ""}
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>

          {/* Comparison table */}
          <div style={s.card}>
            <div style={{ ...s.sectionTitle, fontSize: 16, marginTop: 0 }}>Proportions Table</div>
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                <thead>
                  <tr>
                    <th style={thStyle}>Group</th>
                    {results.map((r) => (
                      <th key={r.job_id} style={thStyle}>{r.sample_name}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {allGroups.map((g) => (
                    <tr key={g}>
                      <td style={{ ...tdStyle, display: "flex", alignItems: "center", gap: 6 }}>
                        <span style={{ width: 8, height: 8, borderRadius: "50%", background: groupColor(g), display: "inline-block", flexShrink: 0 }} />
                        {g}
                      </td>
                      {results.map((r) => {
                        const v = r.proportions[g] || 0;
                        return (
                          <td key={r.job_id} style={{ ...tdStyle, fontWeight: v > 0.1 ? 600 : 400, color: v > 0.1 ? "#e6edf3" : "#8b949e" }}>
                            {v > 0.005 ? `${(v * 100).toFixed(1)}%` : "-"}
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* PCA overlay if any result has PCA data */}
          {results.some((r) => r.pca?.query) && (
            <PCAPlot
              pca={results.find((r) => r.pca?.query)?.pca}
              sampleName={results.find((r) => r.pca?.query)?.sample_name}
              extraQueries={results.filter((r) => r.pca?.query).slice(1)}
            />
          )}
        </div>
      )}
    </div>
  );
}

const thStyle = { textAlign: "left", padding: "8px 12px", borderBottom: "1px solid #30363d", color: "#8b949e", fontWeight: 500 };
const tdStyle = { padding: "8px 12px", borderBottom: "1px solid #21262d", color: "#c9d1d9" };

/* ═══════════════════════════════════════════════════════════════
   Overview Tab
   ═══════════════════════════════════════════════════════════════ */
function MethodologySection() {
  const [open, setOpen] = useState(false);
  return (
    <div>
      <div style={{ ...s.sectionTitle, fontSize: 15, margin: "24px 0 12px", cursor: "pointer", display: "flex", alignItems: "center", gap: 8 }}
        onClick={() => setOpen(!open)}>
        Methodology
        <span style={{ fontSize: 12, color: "#8b949e" }}>{open ? "▲" : "▼"}</span>
      </div>
      {open && (
        <div style={{ ...s.card, fontSize: 13, color: "#8b949e", lineHeight: 1.8 }}>
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontWeight: 600, color: "#e6edf3", marginBottom: 4 }}>Reference Panel</div>
            The gnomAD v3.1 HGDP+1kGP joint callset contains 4,091 high-coverage whole-genome sequenced
            samples from 78 populations across 8 continental groups. The panel includes 157 Middle Eastern
            samples (Druze, Palestinian, Bedouin, Mozabite) essential for detecting Ashkenazi Jewish ancestry.
            Coordinates are GRCh38. Variants are LD-pruned (r² &lt; 0.2, 1000-variant windows) yielding ~240K
            independent SNPs.
          </div>
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontWeight: 600, color: "#e6edf3", marginBottom: 4 }}>Pipeline Steps</div>
            <ol style={{ margin: "4px 0", paddingLeft: 20 }}>
              <li><strong>Variant Extraction</strong> — For BAM/CRAM: bcftools mpileup at reference panel
              positions, then bcftools call. For VCF: normalize, filter biallelic SNPs.</li>
              <li><strong>Allele Alignment</strong> — Intersect with reference variants, align REF/ALT alleles,
              resolve strand mismatches.</li>
              <li><strong>Merge &amp; PCA</strong> — Merge sample with reference panel using PLINK 1.9, apply
              QC filters (mind 0.1, geno 0.1, maf 0.01), compute 20 principal components via PLINK2.</li>
              <li><strong>Rye Decomposition</strong> — Non-negative least squares (NNLS) decomposition using
              the Rye algorithm (Conley &amp; Rishishwar, 2021). Optimizes PC weights and shrinkage parameters
              across 50 rounds × 50 iterations to minimize reference classification error.</li>
              <li><strong>ROH Analysis</strong> — Runs of Homozygosity detection (VCF/gVCF only) using PLINK
              1.9 with 300kb minimum, 50-SNP windows. Flags population bottleneck (&gt;50 Mb) and
              consanguinity (&gt;100 Mb).</li>
            </ol>
          </div>
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontWeight: 600, color: "#e6edf3", marginBottom: 4 }}>Interpretation</div>
            Admixture is flagged when no single component exceeds 85%. Ashkenazi Jewish (ASJ) pattern detection
            triggers when European ancestry is 25–65% and Middle Eastern is 15–50%. Multi-way admixture is
            flagged for 3+ components above 5%.
          </div>
          <div>
            <div style={{ fontWeight: 600, color: "#e6edf3", marginBottom: 4 }}>Limitations</div>
            Results are population-level estimates, not genealogical. The 8-group decomposition is coarser than
            commercial tests. Proportions depend on reference panel composition — populations not represented
            (e.g., Roma, Ethiopian Jewish) may be assigned to the closest available group. BAM input requires
            sufficient coverage for reliable variant calling.
          </div>
        </div>
      )}
    </div>
  );
}

function OverviewTab({ refStatus, refDetail, onStartAnalysis, history, viewJob }) {
  const ready = refStatus?.ready;
  const stats = refDetail?.stats;
  const groups = refDetail?.groups;
  const pops = refDetail?.populations;
  const tools = refDetail?.tool_versions;
  const [showAllPops, setShowAllPops] = useState(false);
  const popEntries = pops ? Object.entries(pops) : [];
  const shownPops = showAllPops ? popEntries : popEntries.slice(0, 12);

  const completedJobs = (history || []).filter((h) => h.status === "complete" && h.result_summary);

  return (
    <div>
      {/* Getting started (empty state) */}
      {completedJobs.length === 0 && (
        <div style={{ ...s.card, textAlign: "center", padding: "40px 24px" }}>
          <div style={{ fontSize: 48, marginBottom: 12 }}>🧬</div>
          <div style={{ fontSize: 18, fontWeight: 600, color: "#e6edf3", marginBottom: 8 }}>Welcome to 23andClaude Ancestry</div>
          <p style={{ fontSize: 14, color: "#8b949e", lineHeight: 1.6, maxWidth: 460, margin: "0 auto 24px" }}>
            Analyze whole-genome sequencing data to discover ancestral composition across 8 continental groups
            using the gnomAD HGDP+1kGP reference panel with 4,091 samples from 78 populations.
          </p>
          <div style={{ display: "flex", gap: 16, justifyContent: "center", flexWrap: "wrap", marginBottom: 24 }}>
            {[["1", "Select a BAM or VCF file", "📁"], ["2", "Pipeline runs (~20 min for BAM)", "⚙️"], ["3", "View ancestry proportions + PCA", "📊"]].map(([n, text, icon]) => (
              <div key={n} style={{ background: "#0d1117", border: "1px solid #21262d", borderRadius: 8, padding: "16px 20px", width: 180, textAlign: "center" }}>
                <div style={{ fontSize: 24, marginBottom: 6 }}>{icon}</div>
                <div style={{ fontSize: 12, color: "#58a6ff", fontWeight: 600, marginBottom: 4 }}>Step {n}</div>
                <div style={{ fontSize: 12, color: "#8b949e" }}>{text}</div>
              </div>
            ))}
          </div>
          <button style={{ ...s.btn, ...s.btnPrimary, padding: "12px 32px", fontSize: 15 }} onClick={onStartAnalysis}>
            Start Your First Analysis
          </button>
        </div>
      )}

      {/* Recent analyses dashboard */}
      {completedJobs.length > 0 && (
        <div style={{ marginBottom: 24 }}>
          <div style={{ ...s.sectionTitle, margin: "0 0 12px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <span>Completed Analyses ({completedJobs.length})</span>
            <button style={{ ...s.btn, ...s.btnPrimary, padding: "6px 16px", fontSize: 13 }} onClick={onStartAnalysis}>
              + New Analysis
            </button>
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(230px, 1fr))", gap: 10 }}>
            {completedJobs.map((h) => (
              <div key={h.job_id}
                style={{ background: "#161b22", border: "1px solid #30363d", borderRadius: 8, padding: "14px 16px", cursor: "pointer", transition: "border-color 0.15s" }}
                onClick={() => viewJob(h.job_id)}
                onMouseEnter={(e) => e.currentTarget.style.borderColor = "#58a6ff"}
                onMouseLeave={(e) => e.currentTarget.style.borderColor = "#30363d"}>
                <div style={{ fontSize: 15, fontWeight: 600, color: "#e6edf3", marginBottom: 6 }}>{h.sample_name}</div>
                <div style={s.compBar}>
                  {h.result_summary?.proportions ? Object.entries(h.result_summary.proportions).sort((a, b) => b[1] - a[1]).map(([g, v]) => (
                    <div key={g} style={{ ...s.compSegment, width: `${Math.max(v * 100, 1)}%`, background: groupColor(g), height: 20, fontSize: 9 }}>
                      {v > 0.1 ? `${(v * 100).toFixed(0)}%` : ""}
                    </div>
                  )) : null}
                </div>
                <div style={{ fontSize: 12, color: "#8b949e" }}>
                  {h.result_summary.primary} ({fmtPct(h.result_summary.primary_pct)}%)
                  {h.result_summary.is_admixed ? " · Admixed" : ""}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      <div style={s.card}>
        <div style={{ fontSize: 16, fontWeight: 600, color: "#e6edf3", marginBottom: 6 }}>Reference Panel</div>
        <p style={{ fontSize: 13, color: "#8b949e", margin: "0 0 16px", lineHeight: 1.5 }}>
          gnomAD HGDP + 1000 Genomes reference panel for ancestry inference.
        </p>
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 16 }}>
          <span style={{ width: 10, height: 10, borderRadius: "50%", background: ready ? "#3fb950" : "#f85149" }} />
          <span style={{ fontSize: 14, color: ready ? "#3fb950" : "#f85149" }}>{ready ? "Ready" : "Not Ready"}</span>
        </div>
        {stats && (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(140px, 1fr))", gap: 12 }}>
            {[[stats.variant_count?.toLocaleString(), "Variants"], [stats.sample_count?.toLocaleString(), "Samples"],
              [stats.population_count, "Populations"], [stats.group_count, "Groups"],
              [`${stats.total_size_gb} GB`, "Total Size"]].map(([v, l]) => (
              <div key={l} style={s.statBox}>
                <div style={{ fontSize: 20, fontWeight: 700, color: "#e6edf3" }}>{v}</div>
                <div style={{ fontSize: 12, color: "#8b949e" }}>{l}</div>
              </div>
            ))}
          </div>
        )}
      </div>

      {groups && (
        <div>
          <div style={{ ...s.sectionTitle, fontSize: 15, margin: "24px 0 12px" }}>Ancestry Groups ({Object.keys(groups).length})</div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(150px, 1fr))", gap: 8 }}>
            {Object.entries(groups).map(([g, n]) => (
              <div key={g} style={{ background: "#161b22", border: "1px solid #21262d", borderRadius: 8, padding: "10px 14px", display: "flex", alignItems: "center", gap: 10 }}>
                <div style={{ width: 10, height: 10, borderRadius: "50%", background: groupColor(g), flexShrink: 0 }} />
                <div>
                  <div style={{ fontSize: 13, color: "#e6edf3", fontWeight: 500 }}>{g}</div>
                  <div style={{ fontSize: 11, color: "#8b949e" }}>{n} samples</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {popEntries.length > 0 && (
        <div>
          <div style={{ ...s.sectionTitle, fontSize: 15, margin: "24px 0 12px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <span>Individual Populations ({popEntries.length})</span>
            {popEntries.length > 12 && (
              <button style={{ ...s.btn, ...s.btnSecondary, padding: "4px 12px", fontSize: 12 }}
                onClick={() => setShowAllPops(!showAllPops)}>
                {showAllPops ? "Show less" : `Show all ${popEntries.length}`}
              </button>
            )}
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(150px, 1fr))", gap: 6 }}>
            {shownPops.map(([p, n]) => (
              <div key={p} style={{ background: "#0d1117", border: "1px solid #21262d", borderRadius: 6, padding: "8px 12px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <span style={{ fontSize: 12, color: "#c9d1d9" }}>{p}</span>
                <span style={{ fontSize: 12, color: "#8b949e", fontWeight: 600 }}>{n}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {tools && (
        <div>
          <div style={{ ...s.sectionTitle, fontSize: 15, margin: "24px 0 12px" }}>Tools</div>
          <div style={{ ...s.card, padding: "12px 20px" }}>
            {Object.entries(tools).map(([t, v]) => (
              <div key={t} style={s.techRow}>
                <span style={{ color: "#8b949e" }}>{t}</span>
                <span style={{ fontSize: 12, color: v ? "#c9d1d9" : "#f85149" }}>{v || "not found"}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Methodology */}
      <MethodologySection />

      <div style={{ textAlign: "center", marginTop: 32 }}>
        <button style={{ ...s.btn, ...s.btnPrimary }} onClick={onStartAnalysis}>Start Analysis</button>
      </div>
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════
   Batch Analyze
   ═══════════════════════════════════════════════════════════════ */
/* ═══════════════════════════════════════════════════════════════
   History Tab with auto-refresh
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
   Main App
   ═══════════════════════════════════════════════════════════════ */
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

export default function App() {
  const [toasts, setToasts] = useState([]);
  _setToasts = setToasts;

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
          <a href="/" style={s.backLink}>← Dashboard</a>
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
                      setFastaPath("/data/reference/GRCh38.fa");
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
                    <input style={s.input} value={sampleName} onChange={(e) => setSampleName(e.target.value)} placeholder="e.g., Sample_WGS" />
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
                                  if (!fastaPath.trim()) setFastaPath("/data/reference/GRCh38.fa");
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
