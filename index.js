const functions = require('@google-cloud/functions-framework');
const { Storage } = require('@google-cloud/storage');
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const { createCanvas, registerFont, loadImage } = require('canvas');

const storage = new Storage();
const cacheBucket = storage.bucket('ranktop-v-cache');
const outputBucket = storage.bucket('ranktop-v');
const thumbnailBucket = storage.bucket('ranktop-v-thumb');

// ─────────────────────────────────────────────────────────────────────────────
// LAYOUT CONFIG — default values used when no custom layoutConfig is passed
// from the client. Clients can override any/all of these fields.
// ─────────────────────────────────────────────────────────────────────────────
const DEFAULT_LAYOUT_CONFIG = {
  fontPath: '/usr/share/fonts/truetype/custom/font.ttf',
  chineseFont: 'Noto Sans CJK SC',
  rankColors: ['#FFD700', '#C0C0C0', '#CD7F32', 'white', 'white'],

  // Title box
  titleFontSize: 100,
  titleLineSpacing: 30,
  titleBoxWidth: 980,
  titleMaxLines: 2,
  titleBoxTopPadding: 30,
  titleBoxBottomPadding: 40,

  // Title backdrop: 'black' | 'white' | 'blurred'
  titleBackdrop: 'black',

  // Title word coloring — array of { word, color } pairs.
  // Words not listed here render in white (or black if backdrop is white).
  // Example: [{ word: 'BEST', color: '#FFD700' }, { word: 'WORST', color: '#FF4444' }]
  titleWordColors: [],

  // Default title text color (used for words not in titleWordColors)
  titleDefaultColor: 'white',

  // Subtitle — small line of text below the title (e.g. "subscribe!" or "the last one is crazy!")
  subtitle: '',
  subtitleFontSize: 44,
  subtitleColor: '#CCCCCC',
  subtitleTopMargin: 10, // gap between title bottom and subtitle

  // Rank entries
  rankFontSize: 60,
  rankSpacing: 140,
  rankPaddingY: 80,
  rankNumX: 45,
  rankTextX: 125,
  rankBoxWidth: 830,
  rankMaxLines: 1,

  // Text rendering
  textOutlineWidth: 18,
  shadowBlur: 25,
  shadowColor: 'rgba(0,0,0,0.8)',

  // Ranktop watermark (bottom-right)
  watermarkText: 'ranktop.net',
  watermarkFontSize: 48,
  watermarkPadding: 20,
  watermarkOpacity: 0.6,

  // Creator watermark (bottom-center) — leave empty to disable
  creatorWatermark: '',
  creatorWatermarkFontSize: 44,
  creatorWatermarkOpacity: 0.7,
  creatorWatermarkColor: 'white',
  creatorWatermarkBottomPadding: 80, // distance from very bottom

  // Style preset: 'default' | 'viral' | 'minimal' | 'dark'
  // Preset values are applied first; individual fields above then override.
  stylePreset: 'default',
};

// ─────────────────────────────────────────────────────────────────────────────
// STYLE PRESETS — merged over DEFAULT_LAYOUT_CONFIG when stylePreset is set.
// ─────────────────────────────────────────────────────────────────────────────
const STYLE_PRESETS = {
  default: {},
  viral: {
    titleBackdrop: 'black',
    textOutlineWidth: 22,
    shadowBlur: 35,
    shadowColor: 'rgba(0,0,0,0.9)',
    rankColors: ['#FFD700', '#C0C0C0', '#CD7F32', '#FF6B6B', '#FF6B6B'],
    subtitleColor: '#FFD700',
  },
  minimal: {
    titleBackdrop: 'white',
    titleDefaultColor: 'black',
    textOutlineWidth: 0,
    shadowBlur: 0,
    shadowColor: 'rgba(0,0,0,0)',
    rankColors: ['#222222', '#444444', '#666666', '#888888', '#888888'],
    watermarkOpacity: 0.3,
    subtitleColor: '#555555',
  },
  dark: {
    titleBackdrop: 'black',
    titleDefaultColor: 'white',
    textOutlineWidth: 14,
    shadowBlur: 30,
    shadowColor: 'rgba(0,0,0,1)',
    subtitleColor: '#AAAAAA',
  },
};

// Build the effective layout config by merging: defaults → preset → client overrides
function resolveLayoutConfig(clientConfig = {}) {
  const preset = clientConfig.stylePreset || DEFAULT_LAYOUT_CONFIG.stylePreset;
  const presetOverrides = STYLE_PRESETS[preset] || {};
  return { ...DEFAULT_LAYOUT_CONFIG, ...presetOverrides, ...clientConfig };
}

// Module-level config — replaced per-request in the HTTP handler
let LAYOUT_CONFIG = resolveLayoutConfig();

const emojiCache = new Map();
if (fs.existsSync(DEFAULT_LAYOUT_CONFIG.fontPath)) {
  registerFont(DEFAULT_LAYOUT_CONFIG.fontPath, { family: 'CustomFont' });
}

// ─────────────────────────────────────────────────────────────────────────────
// Status Management
// ─────────────────────────────────────────────────────────────────────────────
async function updateStatusFile(postId, status, payload = {}) {
  try {
    const file = outputBucket.file(`${postId}.json`);
    const data = JSON.stringify({ status, updatedAt: Date.now(), ...payload });
    await file.save(data, {
      contentType: 'application/json',
      resumable: false,
      metadata: { cacheControl: 'no-cache' }
    });
  } catch (e) {
    console.warn("Failed to update status file:", e.message);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Database Webhook
// ─────────────────────────────────────────────────────────────────────────────
async function notifyWebsite(postId, status, errorMessage = null, req = null) {
  let baseUrl = "https://ranktop.net";
  if (req && req.headers['x-callback-url']) {
    baseUrl = req.headers['x-callback-url'].replace(/\/$/, "");
  }
  const url = `${baseUrl}/api/internal/update-post`;
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-internal-secret': process.env.INTERNAL_SECRET
      },
      body: JSON.stringify({ postId, status, errorMessage })
    });
    if (!res.ok) console.error(`Webhook rejected (${res.status})`);
  } catch (err) {
    console.error("Webhook notification failed:", err.message);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────────────────────────────────────
function getEmojiUrl(emoji) {
  const codePoints = Array.from(emoji).map(c => c.codePointAt(0).toString(16)).join('-');
  return `https://cdn.jsdelivr.net/gh/jdecked/twemoji@15.0.3/assets/72x72/${codePoints}.png`;
}

function getFontForChar(char) {
  if (/\p{Extended_Pictographic}/u.test(char)) return 'Emoji';
  if (/[\u4e00-\u9fa5]|[\u3040-\u30ff]|[\uff00-\uffef]/.test(char)) return LAYOUT_CONFIG.chineseFont;
  return 'CustomFont';
}

function segmentTextByFont(text) {
  const segments = [];
  if (!text) return segments;
  let currentSegment = { text: '', font: '' };
  for (const char of text) {
    const fontNeeded = getFontForChar(char);
    if (currentSegment.text === '') {
      currentSegment = { text: char, font: fontNeeded };
    } else if (currentSegment.font === fontNeeded) {
      currentSegment.text += char;
    } else {
      segments.push(currentSegment);
      currentSegment = { text: char, font: fontNeeded };
    }
  }
  if (currentSegment.text) segments.push(currentSegment);
  return segments;
}

function measureMixedText(ctx, text, fontSize) {
  const segments = segmentTextByFont(text);
  let totalWidth = 0;
  segments.forEach(s => {
    if (s.font === 'Emoji') {
      totalWidth += (fontSize * Array.from(s.text).length);
    } else {
      ctx.font = `${fontSize}px "${s.font}"`;
      totalWidth += ctx.measureText(s.text).width;
    }
  });
  return totalWidth;
}

async function drawMixedText(ctx, text, x, y, fontSize, fillStyle, strokeStyle = null, lineWidth = 0) {
  const segments = segmentTextByFont(text);
  let currentX = x;
  for (const s of segments) {
    if (s.font === 'Emoji') {
      const emojis = Array.from(s.text);
      for (const emoji of emojis) {
        try {
          const url = getEmojiUrl(emoji);
          let img = emojiCache.get(url);
          if (!img) { img = await loadImage(url); emojiCache.set(url, img); }
          ctx.drawImage(img, currentX, y + (fontSize * 0.1), fontSize, fontSize);
        } catch (e) { console.warn(`Emoji Load Failed: ${emoji}`); }
        currentX += fontSize;
      }
    } else {
      ctx.font = `${fontSize}px "${s.font}"`;
      if (strokeStyle && lineWidth > 0) {
        ctx.strokeStyle = strokeStyle;
        ctx.lineWidth = lineWidth;
        ctx.strokeText(s.text, currentX, y);
      }
      ctx.fillStyle = fillStyle;
      ctx.fillText(s.text, currentX, y);
      currentX += ctx.measureText(s.text).width;
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-word colored title drawing
//
// titleWordColors is an array of { word, color } objects. Words are matched
// case-insensitively against each whitespace-delimited token in the line.
// Unmatched words use titleDefaultColor.
// ─────────────────────────────────────────────────────────────────────────────
function buildWordColorMap(wordColors) {
  const map = new Map();
  for (const { word, color } of (wordColors || [])) {
    map.set(word.toLowerCase(), color);
  }
  return map;
}

async function drawColoredTitleLine(ctx, line, x, y, fontSize) {
  const wordColorMap = buildWordColorMap(LAYOUT_CONFIG.titleWordColors);
  const outlineWidth = LAYOUT_CONFIG.textOutlineWidth;
  const strokeColor = LAYOUT_CONFIG.titleBackdrop === 'white' ? 'rgba(255,255,255,0.6)' : 'black';

  // Apply shadow
  ctx.shadowColor = LAYOUT_CONFIG.shadowColor;
  ctx.shadowBlur = LAYOUT_CONFIG.shadowBlur;
  ctx.shadowOffsetX = 0;
  ctx.shadowOffsetY = 0;

  const words = line.split(' ');
  let currentX = x;

  for (let wi = 0; wi < words.length; wi++) {
    const word = words[wi];
    const color = wordColorMap.get(word.toLowerCase()) || LAYOUT_CONFIG.titleDefaultColor;
    const displayWord = wi < words.length - 1 ? word + ' ' : word;
    await drawMixedText(ctx, displayWord, currentX, y, fontSize, color, strokeColor, outlineWidth);
    currentX += measureMixedText(ctx, displayWord, fontSize);
  }

  // Reset shadow
  ctx.shadowBlur = 0;
  ctx.shadowColor = 'rgba(0,0,0,0)';
}

// ─────────────────────────────────────────────────────────────────────────────
// Title backdrop rendering
//
// 'black' and 'white' are solid fills.
// 'blurred' draws a semi-transparent dark gradient — FFmpeg handles the actual
// blur of the underlying video frame; we just draw a darkened overlay so text
// stays readable on any background.
// ─────────────────────────────────────────────────────────────────────────────
function drawTitleBackdrop(ctx, boxH) {
  const backdrop = LAYOUT_CONFIG.titleBackdrop;
  if (backdrop === 'black') {
    ctx.fillStyle = 'black';
    ctx.fillRect(0, 0, 1080, boxH);
  } else if (backdrop === 'white') {
    ctx.fillStyle = 'white';
    ctx.fillRect(0, 0, 1080, boxH);
  } else if (backdrop === 'blurred') {
    // Semi-transparent dark gradient that blends with the video blur underneath
    const grad = ctx.createLinearGradient(0, 0, 0, boxH);
    grad.addColorStop(0, 'rgba(0,0,0,0.82)');
    grad.addColorStop(1, 'rgba(0,0,0,0.55)');
    ctx.fillStyle = grad;
    ctx.fillRect(0, 0, 1080, boxH);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Subtitle drawing — rendered immediately below the title text inside the box
// ─────────────────────────────────────────────────────────────────────────────
async function drawSubtitle(ctx, subtitleY) {
  if (!LAYOUT_CONFIG.subtitle) return;
  const subFontSize = LAYOUT_CONFIG.subtitleFontSize;
  const subW = measureMixedText(ctx, LAYOUT_CONFIG.subtitle, subFontSize);
  const subX = (1080 - subW) / 2;
  const outlineColor = LAYOUT_CONFIG.titleBackdrop === 'white' ? 'rgba(0,0,0,0.15)' : 'rgba(0,0,0,0.7)';
  await drawMixedText(
    ctx, LAYOUT_CONFIG.subtitle, subX, subtitleY, subFontSize,
    LAYOUT_CONFIG.subtitleColor, outlineColor, LAYOUT_CONFIG.textOutlineWidth * 0.5
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Creator watermark — bottom-center
// ─────────────────────────────────────────────────────────────────────────────
async function drawCreatorWatermark(ctx) {
  if (!LAYOUT_CONFIG.creatorWatermark) return;
  const fontSize = LAYOUT_CONFIG.creatorWatermarkFontSize;
  const wmW = measureMixedText(ctx, LAYOUT_CONFIG.creatorWatermark, fontSize);
  const x = (1080 - wmW) / 2;
  const y = 1920 - fontSize - LAYOUT_CONFIG.creatorWatermarkBottomPadding;
  ctx.save();
  ctx.globalAlpha = LAYOUT_CONFIG.creatorWatermarkOpacity;
  await drawMixedText(
    ctx, LAYOUT_CONFIG.creatorWatermark, x, y, fontSize,
    LAYOUT_CONFIG.creatorWatermarkColor, 'black', LAYOUT_CONFIG.textOutlineWidth * 0.6
  );
  ctx.restore();
}

// ─────────────────────────────────────────────────────────────────────────────
// fitTextToBox — unchanged
// ─────────────────────────────────────────────────────────────────────────────
function fitTextToBox(text, boxWidth, maxLines, initialFontSize) {
  const canvas = createCanvas(boxWidth, 100);
  const ctx = canvas.getContext('2d');
  for (let fontSize = initialFontSize; fontSize >= 1; fontSize -= 2) {
    const words = text.split(' '), lines = []; let currentLine = '';
    for (const word of words) {
      const test = currentLine ? `${currentLine} ${word}` : word;
      if (measureMixedText(ctx, test, fontSize) <= boxWidth) currentLine = test;
      else { lines.push(currentLine); currentLine = word; }
    }
    if (currentLine) lines.push(currentLine);
    if (lines.length <= maxLines) return { fontSize, lines };
  }
  return { fontSize: 10, lines: [text] };
}

// ─────────────────────────────────────────────────────────────────────────────
// computeTitleBoxH — now accounts for optional subtitle height
// ─────────────────────────────────────────────────────────────────────────────
function computeTitleBoxH(title) {
  const titleRes = fitTextToBox(title, LAYOUT_CONFIG.titleBoxWidth, LAYOUT_CONFIG.titleMaxLines, LAYOUT_CONFIG.titleFontSize);
  const textH = (titleRes.lines.length * titleRes.fontSize) + ((titleRes.lines.length - 1) * LAYOUT_CONFIG.titleLineSpacing);
  const subtitleH = LAYOUT_CONFIG.subtitle
    ? LAYOUT_CONFIG.subtitleTopMargin + LAYOUT_CONFIG.subtitleFontSize
    : 0;
  const boxH = LAYOUT_CONFIG.titleBoxTopPadding + textH + subtitleH + LAYOUT_CONFIG.titleBoxBottomPadding;
  return { titleRes, boxH, textH, subtitleH };
}

// ─────────────────────────────────────────────────────────────────────────────
// drawTitleBlock — shared helper used by all overlay creators
// Draws backdrop, title lines (with per-word coloring + shadow), subtitle,
// and returns the bottom Y of the title box.
// ─────────────────────────────────────────────────────────────────────────────
async function drawTitleBlock(ctx, title) {
  const { titleRes, boxH, textH } = computeTitleBoxH(title);

  drawTitleBackdrop(ctx, boxH);

  // Center title lines vertically within the non-subtitle portion
  const subtitleH = LAYOUT_CONFIG.subtitle
    ? LAYOUT_CONFIG.subtitleTopMargin + LAYOUT_CONFIG.subtitleFontSize
    : 0;
  const titleAreaH = boxH - subtitleH;
  let currY = (titleAreaH - textH) / 2;

  for (const line of titleRes.lines) {
    const lw = measureMixedText(ctx, line, titleRes.fontSize);
    const lineX = (1080 - lw) / 2;
    await drawColoredTitleLine(ctx, line, lineX, currY, titleRes.fontSize);
    currY += titleRes.fontSize + LAYOUT_CONFIG.titleLineSpacing;
  }

  // Subtitle sits just below the title text, still inside the box
  if (LAYOUT_CONFIG.subtitle) {
    await drawSubtitle(ctx, currY + LAYOUT_CONFIG.subtitleTopMargin);
  }

  return boxH;
}

// ─────────────────────────────────────────────────────────────────────────────
// drawRantopWatermark — bottom-right, unchanged position
// ─────────────────────────────────────────────────────────────────────────────
async function drawRantopWatermark(ctx) {
  const wmW = measureMixedText(ctx, LAYOUT_CONFIG.watermarkText, LAYOUT_CONFIG.watermarkFontSize);
  ctx.save();
  ctx.globalAlpha = LAYOUT_CONFIG.watermarkOpacity;
  ctx.shadowColor = LAYOUT_CONFIG.shadowColor;
  ctx.shadowBlur = LAYOUT_CONFIG.shadowBlur * 0.6;
  await drawMixedText(
    ctx, LAYOUT_CONFIG.watermarkText,
    1080 - wmW - LAYOUT_CONFIG.watermarkPadding,
    1920 - LAYOUT_CONFIG.watermarkFontSize - LAYOUT_CONFIG.watermarkPadding,
    LAYOUT_CONFIG.watermarkFontSize, 'white', 'black', LAYOUT_CONFIG.textOutlineWidth
  );
  ctx.shadowBlur = 0;
  ctx.restore();
}

// ─────────────────────────────────────────────────────────────────────────────
// createTextOverlayImage — auto-stitch pipeline
// ─────────────────────────────────────────────────────────────────────────────
async function createTextOverlayImage(title, ranks, ranksToShow) {
  const canvas = createCanvas(1080, 1920), ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, 1080, 1920);
  ctx.textBaseline = 'top'; ctx.textAlign = 'left';

  const boxH = await drawTitleBlock(ctx, title);

  for (let i = 0; i < ranksToShow; i++) {
    const idx = (ranks.length - ranksToShow) + i;
    const y = LAYOUT_CONFIG.rankPaddingY + boxH + (idx * LAYOUT_CONFIG.rankSpacing);
    const rRes = fitTextToBox(ranks[idx], LAYOUT_CONFIG.rankBoxWidth, LAYOUT_CONFIG.rankMaxLines, LAYOUT_CONFIG.rankFontSize);

    ctx.shadowColor = LAYOUT_CONFIG.shadowColor;
    ctx.shadowBlur = LAYOUT_CONFIG.shadowBlur;
    ctx.shadowOffsetX = 0; ctx.shadowOffsetY = 0;

    ctx.font = `${LAYOUT_CONFIG.rankFontSize}px "CustomFont"`;
    ctx.strokeStyle = 'black'; ctx.lineWidth = LAYOUT_CONFIG.textOutlineWidth;
    ctx.strokeText(`${idx + 1}.`, LAYOUT_CONFIG.rankNumX, y);
    ctx.fillStyle = LAYOUT_CONFIG.rankColors[idx] || 'white';
    ctx.fillText(`${idx + 1}.`, LAYOUT_CONFIG.rankNumX, y);

    ctx.shadowBlur = 0; ctx.shadowColor = 'rgba(0,0,0,0)';

    await drawMixedText(ctx, rRes.lines[0], LAYOUT_CONFIG.rankTextX, y + ((LAYOUT_CONFIG.rankFontSize - rRes.fontSize) / 2), rRes.fontSize, 'white', 'black', LAYOUT_CONFIG.textOutlineWidth);
  }

  await drawRantopWatermark(ctx);
  await drawCreatorWatermark(ctx);

  return canvas;
}

// ─────────────────────────────────────────────────────────────────────────────
// createBaseOverlayImage — pre-edited pipeline: title + watermarks only
// ─────────────────────────────────────────────────────────────────────────────
async function createBaseOverlayImage(title) {
  const canvas = createCanvas(1080, 1920), ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, 1080, 1920);
  ctx.textBaseline = 'top'; ctx.textAlign = 'left';

  await drawTitleBlock(ctx, title);
  await drawRantopWatermark(ctx);
  await drawCreatorWatermark(ctx);

  return canvas;
}

// ─────────────────────────────────────────────────────────────────────────────
// createRankOverlayImage — pre-edited pipeline: single rank entry
// ─────────────────────────────────────────────────────────────────────────────
async function createRankOverlayImage(ranks, rankIndex, boxH) {
  const canvas = createCanvas(1080, 1920), ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, 1080, 1920);
  ctx.textBaseline = 'top'; ctx.textAlign = 'left';

  const y = LAYOUT_CONFIG.rankPaddingY + boxH + (rankIndex * LAYOUT_CONFIG.rankSpacing);
  const rRes = fitTextToBox(ranks[rankIndex], LAYOUT_CONFIG.rankBoxWidth, LAYOUT_CONFIG.rankMaxLines, LAYOUT_CONFIG.rankFontSize);

  ctx.shadowColor = LAYOUT_CONFIG.shadowColor;
  ctx.shadowBlur = LAYOUT_CONFIG.shadowBlur;
  ctx.shadowOffsetX = 0; ctx.shadowOffsetY = 0;

  ctx.font = `${LAYOUT_CONFIG.rankFontSize}px "CustomFont"`;
  ctx.strokeStyle = 'black'; ctx.lineWidth = LAYOUT_CONFIG.textOutlineWidth;
  ctx.strokeText(`${rankIndex + 1}.`, LAYOUT_CONFIG.rankNumX, y);
  ctx.fillStyle = LAYOUT_CONFIG.rankColors[rankIndex] || 'white';
  ctx.fillText(`${rankIndex + 1}.`, LAYOUT_CONFIG.rankNumX, y);

  ctx.shadowBlur = 0; ctx.shadowColor = 'rgba(0,0,0,0)';

  await drawMixedText(ctx, rRes.lines[0], LAYOUT_CONFIG.rankTextX, y + ((LAYOUT_CONFIG.rankFontSize - rRes.fontSize) / 2), rRes.fontSize, 'white', 'black', LAYOUT_CONFIG.textOutlineWidth);

  return canvas;
}

// ─────────────────────────────────────────────────────────────────────────────
// generateThumbnail
// ─────────────────────────────────────────────────────────────────────────────
async function generateThumbnail(videoPath, outputPath) {
  return spawnWithTimeout('ffmpeg', [
    '-i', videoPath, '-ss', '00:00:01', '-vframes', '1', '-q:v', '2', '-y', outputPath
  ], 30_000, 'Thumbnail');
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared spawn / download helpers
// ─────────────────────────────────────────────────────────────────────────────
function spawnWithTimeout(cmd, args, ms, label = 'Process') {
  return new Promise((resolve, reject) => {
    const proc = spawn(cmd, args);
    let stderr = '';
    proc.stderr.on('data', d => { stderr += d.toString(); });
    proc.on('error', reject);
    proc.on('close', code => {
      clearTimeout(timer);
      code === 0 ? resolve() : reject(new Error(`${label} failed (${code}): ${stderr.slice(-400)}`));
    });
    const timer = setTimeout(() => {
      proc.kill('SIGKILL');
      reject(new Error(`${label} timed out after ${ms / 1000}s`));
    }, ms);
  });
}

function downloadWithTimeout(gcsFile, destination, ms, label = 'Download') {
  return Promise.race([
    gcsFile.download({ destination }),
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`${label} timed out after ${ms / 1000}s`)), ms)
    )
  ]);
}

// ─────────────────────────────────────────────────────────────────────────────
// FFmpeg helpers
// applyOverlay: for auto-stitch — 'blurred' backdrop pipes source through
// a boxblur so the title area looks like a frosted glass effect.
// ─────────────────────────────────────────────────────────────────────────────
function applyOverlay(inputPath, overlayPath, outputPath, boxH = 0) {
  let filter;

  if (LAYOUT_CONFIG.titleBackdrop === 'blurred' && boxH > 0) {
    // Blur only the top region (title box), composite full overlay on top
    filter = [
      `[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[scaled]`,
      `[scaled]split[full][forblur]`,
      `[forblur]crop=1080:${Math.ceil(boxH)}:0:0,boxblur=20:5[blurred_top]`,
      `[full][blurred_top]overlay=0:0[with_blur]`,
      `[1:v]scale=1080:1920[ov]`,
      `[with_blur][ov]overlay=0:0`,
    ].join(';');
  } else {
    filter = `[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[v];[1:v]scale=1080:1920[ov];[v][ov]overlay=0:0`;
  }

  return spawnWithTimeout('ffmpeg', [
    '-i', inputPath, '-i', overlayPath,
    '-filter_complex', filter,
    '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23', '-c:a', 'aac',
    '-movflags', '+faststart', '-y', outputPath
  ], 300_000, 'Overlay');
}

function stitchClips(listPath, outputPath) {
  return spawnWithTimeout('ffmpeg', [
    '-f', 'concat', '-safe', '0', '-i', listPath,
    '-c', 'copy', '-movflags', '+faststart', '-y', outputPath
  ], 120_000, 'Stitch');
}

function cutSegment(sourcePath, startSec, endSec, outputPath) {
  return spawnWithTimeout('ffmpeg', [
    '-ss', String(startSec), '-i', sourcePath,
    '-t', String(endSec - startSec),
    '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23',
    '-c:a', 'aac', '-movflags', '+faststart', '-y', outputPath
  ], 300_000, 'Cut');
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline: Auto-stitch (N separate uploaded clips)
// ─────────────────────────────────────────────────────────────────────────────
async function processAutoStitch(req, res, { postId, title, ranks, filePaths }) {
  const tempFiles = [];
  try {
    await updateStatusFile(postId, 'PROCESSING', { progress: 5 });
    const parsedRanks = typeof ranks === 'string' ? JSON.parse(ranks) : ranks;

    const local = await Promise.all(filePaths.map(async (fp, i) => {
      const p = `/tmp/in_${i}_${uuidv4()}${path.extname(fp) || '.mp4'}`;
      await downloadWithTimeout(cacheBucket.file(fp), p, 120_000, `Download clip ${i}`);
      tempFiles.push(p);
      return p;
    }));

    const { boxH } = computeTitleBoxH(title);
    const processed = [];

    for (let i = 0; i < local.length; i++) {
      const prog = 10 + Math.floor((i / local.length) * 60);
      await updateStatusFile(postId, 'PROCESSING', { progress: prog });

      const ovPath = `/tmp/ov_${i}_${uuidv4()}.png`;
      const outPath = `/tmp/proc_${i}_${uuidv4()}.mp4`;
      tempFiles.push(ovPath, outPath);

      const canvas = await createTextOverlayImage(title, parsedRanks, i + 1);
      fs.writeFileSync(ovPath, canvas.toBuffer('image/png'));
      await applyOverlay(local[i], ovPath, outPath, boxH);
      processed.push(outPath);
    }

    await updateStatusFile(postId, 'PROCESSING', { progress: 80 });
    const listPath = `/tmp/l_${uuidv4()}.txt`;
    const finalPath = `/tmp/f_${uuidv4()}.mp4`;
    tempFiles.push(listPath, finalPath);
    fs.writeFileSync(listPath, processed.map(p => `file '${p}'`).join('\n'));
    await stitchClips(listPath, finalPath);

    await updateStatusFile(postId, 'PROCESSING', { progress: 90 });
    const thumbPath = `/tmp/t_${uuidv4()}.jpg`;
    tempFiles.push(thumbPath);
    await generateThumbnail(finalPath, thumbPath);

    await Promise.all([
      outputBucket.upload(finalPath, { destination: `${postId}.mp4` }),
      thumbnailBucket.upload(thumbPath, { destination: `${postId}.jpg`, metadata: { contentType: 'image/jpeg' } })
    ]);

    await updateStatusFile(postId, 'READY', { progress: 100 });
    await notifyWebsite(postId, 'READY', null, req);
    res.status(200).json({ status: 'SUCCESS' });

  } catch (error) {
    console.error("Auto-Stitch Job Error:", error);
    await updateStatusFile(postId, 'FAILED', { error: error.message });
    await notifyWebsite(postId, 'FAILED', error.message, req);
    res.status(500).json({ error: error.message });
  } finally {
    tempFiles.forEach(f => { try { fs.unlinkSync(f); } catch {} });
    emojiCache.clear();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline: Pre-edited (single source file, timed text overlays)
// ─────────────────────────────────────────────────────────────────────────────
async function processPreEdited(req, res, { postId, title, ranks, filePath, timestamps, endTime }) {
  const tempFiles = [];
  try {
    await updateStatusFile(postId, 'PROCESSING', { progress: 5 });
    const parsedRanks = typeof ranks === 'string' ? JSON.parse(ranks) : ranks;
    const parsedEndTime = typeof endTime === 'string' ? parseFloat(endTime) : endTime;

    await updateStatusFile(postId, 'PROCESSING', { progress: 10 });
    const sourcePath = `/tmp/source_${uuidv4()}${path.extname(filePath) || '.mp4'}`;
    await downloadWithTimeout(cacheBucket.file(filePath), sourcePath, 120_000, 'Download source');
    tempFiles.push(sourcePath);

    await updateStatusFile(postId, 'PROCESSING', { progress: 20 });
    const sortedTimestamps = [...timestamps].sort((a, b) => a.time - b.time);

    const { boxH } = computeTitleBoxH(title);

    const basePath = `/tmp/base_${uuidv4()}.png`;
    tempFiles.push(basePath);
    fs.writeFileSync(basePath, (await createBaseOverlayImage(title)).toBuffer('image/png'));

    const rankPaths = [];
    for (let i = 0; i < parsedRanks.length; i++) {
      await updateStatusFile(postId, 'PROCESSING', { progress: 25 + Math.floor((i / parsedRanks.length) * 35) });
      const rankPath = `/tmp/rank_${i}_${uuidv4()}.png`;
      tempFiles.push(rankPath);
      const rankIndex = parsedRanks.length - 1 - i;
      fs.writeFileSync(rankPath, (await createRankOverlayImage(parsedRanks, rankIndex, boxH)).toBuffer('image/png'));
      rankPaths.push({ path: rankPath, rankIndex, timestampSlot: i });
    }

    await updateStatusFile(postId, 'PROCESSING', { progress: 65 });

    const finalPath = `/tmp/f_${uuidv4()}.mp4`;
    tempFiles.push(finalPath);

    const inputArgs = ['-i', sourcePath, '-i', basePath];
    for (const { path } of rankPaths) inputArgs.push('-i', path);

    // For 'blurred' backdrop, blur the video's top region before compositing
    const filterParts = [];
    let scaledLabel;

    if (LAYOUT_CONFIG.titleBackdrop === 'blurred') {
      filterParts.push(
        `[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[scaled]`,
        `[scaled]split[full][forblur]`,
        `[forblur]crop=1080:${Math.ceil(boxH)}:0:0,boxblur=20:5[blurred_top]`,
        `[full][blurred_top]overlay=0:0[v_preblur]`,
      );
      scaledLabel = 'v_preblur';
    } else {
      filterParts.push(
        `[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[v_scaled]`,
      );
      scaledLabel = 'v_scaled';
    }

    filterParts.push(
      `[1:v]scale=1080:1920[base_ov]`,
      `[${scaledLabel}][base_ov]overlay=0:0[v_base]`,
    );

    let prevLabel = 'v_base';
    for (let i = 0; i < rankPaths.length; i++) {
      const { timestampSlot } = rankPaths[i];
      const start = sortedTimestamps[timestampSlot]?.time ?? 0;
      const inputIdx = i + 2;
      filterParts.push(`[${inputIdx}:v]scale=1080:1920[r${i}]`);
      filterParts.push(`[${prevLabel}][r${i}]overlay=0:0:enable='between(t,${start},${parsedEndTime})'[v${i}]`);
      prevLabel = `v${i}`;
    }

    await spawnWithTimeout('ffmpeg', [
      ...inputArgs,
      '-filter_complex', filterParts.join(';'),
      '-map', `[${prevLabel}]`,
      '-map', '0:a?',
      '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23',
      '-c:a', 'aac',
      '-movflags', '+faststart',
      '-y', finalPath
    ], 600_000, 'Pre-edited overlay');

    await updateStatusFile(postId, 'PROCESSING', { progress: 90 });
    const thumbPath = `/tmp/t_${uuidv4()}.jpg`;
    tempFiles.push(thumbPath);
    await generateThumbnail(finalPath, thumbPath);

    await Promise.all([
      outputBucket.upload(finalPath, { destination: `${postId}.mp4` }),
      thumbnailBucket.upload(thumbPath, { destination: `${postId}.jpg`, metadata: { contentType: 'image/jpeg' } })
    ]);

    await updateStatusFile(postId, 'READY', { progress: 100 });
    await notifyWebsite(postId, 'READY', null, req);
    res.status(200).json({ status: 'SUCCESS' });

  } catch (error) {
    console.error("Pre-Edited Job Error:", error);
    await updateStatusFile(postId, 'FAILED', { error: error.message });
    await notifyWebsite(postId, 'FAILED', error.message, req);
    res.status(500).json({ error: error.message });
  } finally {
    tempFiles.forEach(f => { try { fs.unlinkSync(f); } catch {} });
    emojiCache.clear();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Main HTTP Function
// ─────────────────────────────────────────────────────────────────────────────
functions.http('processVideos', async (req, res) => {
  const {
    action, videoCount, sessionId, fileTypes, fileType,
    title, ranks, filePaths, filePath, timestamps, endTime,
    postId, videoMode,
    // NEW: optional layout config from client
    layoutConfig,
  } = req.body;

  // Resolve layout config for this request (client overrides > preset > defaults)
  LAYOUT_CONFIG = resolveLayoutConfig(layoutConfig || {});

  // 1. Check Status
  if (action === 'checkStatus') {
    try {
      const file = outputBucket.file(`${postId}.json`);
      const [exists] = await file.exists();
      if (!exists) return res.json({ status: 'PENDING' });
      const [content] = await file.download();
      return res.json(JSON.parse(content.toString()));
    } catch (e) {
      return res.status(500).json({ status: 'ERROR', error: e.message });
    }
  }

  // 2. Generate Upload URLs (auto-stitch: multiple files)
  if (action === 'getUploadUrls') {
    if (!fileTypes || !Array.isArray(fileTypes)) return res.status(400).json({ error: "Missing fileTypes" });
    const uploadUrls = [], generatedPaths = [];
    for (let i = 0; i < videoCount; i++) {
      const contentType = fileTypes[i] || 'video/mp4';
      const fileName = `${sessionId}/v_${i}.${contentType.split('/')[1] || 'mp4'}`;
      generatedPaths.push(fileName);
      const [url] = await cacheBucket.file(fileName).getSignedUrl({
        version: 'v4', action: 'write', expires: Date.now() + 900000, contentType
      });
      uploadUrls.push({ index: i, url });
    }
    return res.json({ uploadUrls, filePaths: generatedPaths, sessionId });
  }

  // 3. Generate Upload URL (pre-edited: single file)
  if (action === 'getUploadUrl') {
    if (!sessionId || !fileType) return res.status(400).json({ error: "Missing sessionId or fileType" });
    const ext = fileType.split('/')[1] || 'mp4';
    const fileName = `${sessionId}/pre_source.${ext}`;
    const [url] = await cacheBucket.file(fileName).getSignedUrl({
      version: 'v4', action: 'write', expires: Date.now() + 900000, contentType: fileType
    });
    return res.json({ uploadUrl: url, filePath: fileName });
  }

  // 4. Main Processing Job — route by videoMode
  if (videoMode === 'pre-edited') {
    if (!filePath || !postId || !timestamps || !Array.isArray(timestamps) || timestamps.length === 0) {
      return res.status(400).json({ error: "Missing filePath, postId, or timestamps" });
    }
    return processPreEdited(req, res, { postId, title, ranks, filePath, timestamps, endTime });
  } else {
    if (!filePaths || !postId) {
      return res.status(400).json({ error: "Missing filePaths or postId" });
    }
    return processAutoStitch(req, res, { postId, title, ranks, filePaths });
  }
});
