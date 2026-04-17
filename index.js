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
// LOGGING
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Lightweight structured logger. GCR reads stdout/stderr as plain log lines,
 * so we keep it simple: a severity prefix + message. No external dependencies.
 *
 * Usage:
 *   log.info('Downloaded clip', { index: 2, path: '/tmp/...' })
 *   log.warn('Emoji load failed', { emoji: '🔥' })
 *   log.error('FFmpeg crashed', { code: 1, stderr: '...' })
 *   log.time('ffmpeg-overlay-3')   // start a named timer
 *   log.timeEnd('ffmpeg-overlay-3') // logs elapsed ms
 */
const _timers = new Map();
const log = {
  _fmt: (level, msg, meta) => {
    const base = `[${level}] ${msg}`;
    return meta ? `${base} | ${JSON.stringify(meta)}` : base;
  },
  info:    (msg, meta) => console.log(log._fmt('INFO',  msg, meta)),
  warn:    (msg, meta) => console.warn(log._fmt('WARN',  msg, meta)),
  error:   (msg, meta) => console.error(log._fmt('ERROR', msg, meta)),
  debug:   (msg, meta) => console.log(log._fmt('DEBUG', msg, meta)),
  time:    (label)     => _timers.set(label, Date.now()),
  timeEnd: (label, extra) => {
    const start = _timers.get(label);
    if (start) {
      const ms = Date.now() - start;
      _timers.delete(label);
      console.log(log._fmt('PERF', label, { ms, ...extra }));
    }
  },
};

// ─────────────────────────────────────────────────────────────────────────────
// LAYOUT CONFIG
// ─────────────────────────────────────────────────────────────────────────────

/**
 * The base defaults for all layout config fields.
 * These match DEFAULT_VIDEO_STYLE + the hardcoded values from getDerivedVideoSettings
 * (before scaling), so the server always renders at 1080x1920 (scale = 1.0).
 *
 * NOTE: rankColors has 10 entries to support the current 10-rank max.
 */
const DEFAULT_LAYOUT_CONFIG = {
  fontFamily: 'Archivo Expanded Bold',
  chineseFont: 'Noto Sans CJK SC',

  // Title
  titleFontSize: 100,
  titleLineSpacing: 30,
  titleBoxWidth: 980,
  titleMaxLines: 2,
  titleBoxTopPadding: 30,
  titleBoxBottomPadding: 40,
  titleBackdrop: 'black',         // 'none' | 'black' | 'white' | 'blurred'
  titleWordColors: [],
  titleDefaultColor: 'white',
  titleShadowBlur: 25,
  titleShadowColor: 'rgba(0,0,0,0.8)',

  // Subtitle
  subtitle: '',
  subtitleFontSize: 44,
  subtitleColor: '#CCCCCC',
  subtitleTopMargin: 10,

  // Ranks
  rankFontSize: 60,
  rankTextFontSize: 60,
  rankSpacing: 140,
  rankYOffset: 0,
  pushVideoDown: false,
  titleAccentOutline: false,
  rankPaddingY: 80,
  rankNumX: 45,
  rankTextX: 125,
  rankBoxWidth: 830,
  rankMaxLines: 1,
  // 10 entries — covers the full rank max of 10
  rankColors: ['#FFD700', '#C0C0C0', '#CD7F32', 'white', 'white', 'white', 'white', 'white', 'white', 'white'],
  rankShadowBlur: 5,
  rankShadowColor: 'rgba(0,0,0,0.8)',

  // Text styling
  // NOTE: textOutlineWidth is NOT set here — it is derived in getDerivedSettings()
  // based on fontFamily, so it is always computed fresh. If the client explicitly
  // passes textOutlineWidth it will be respected as an override.
  textShadow: true,

  // Watermarks
  watermarkText: 'ranktop.net',
  watermarkFontSize: 48,
  watermarkPadding: 20,
  watermarkOpacity: 0.6,
  creatorWatermark: '',
  creatorWatermarkFontSize: 44,
  creatorWatermarkOpacity: 0.7,
  creatorWatermarkColor: '#FFFFFF',
  creatorWatermarkBottomPadding: 80,

  matchRankColor: false,
};

/**
 * Mirrors the frontend's getDerivedVideoSettings() at scale = 1.0 (full 1080x1920).
 *
 * Priority: explicit client override → computed default.
 * This is the single source of truth for every dimension used by the renderers.
 */
function getDerivedSettings(clientConfig = {}) {
  const SCALE = 1.0;

  // textOutlineWidth: use explicit override if provided, otherwise derive from font
  const baseOutlineWidth = clientConfig.textOutlineWidth != null
    ? clientConfig.textOutlineWidth
    : (clientConfig.fontFamily === 'Arial Regular' ? 9 : 18);

  return {
    // ── Typography ──
    titleFontSize:                 (clientConfig.titleFontSize          ?? 100) * SCALE,
    titleLineSpacing:              (clientConfig.titleLineSpacing        ?? 30)  * SCALE,
    titleBoxWidth:                 (clientConfig.titleBoxWidth           ?? 980) * SCALE,
    titleMaxLines:                  clientConfig.titleMaxLines           ?? 2,
    titleBoxTopPadding:            (clientConfig.titleBoxTopPadding      ?? 30)  * SCALE,
    titleBoxBottomPadding:         (clientConfig.titleBoxBottomPadding   ?? 40)  * SCALE,

    subtitleFontSize:              (clientConfig.subtitleFontSize        ?? 44)  * SCALE,
    subtitleTopMargin:             (clientConfig.subtitleTopMargin       ?? 10)  * SCALE,

    rankFontSize:                  (clientConfig.rankFontSize            ?? 60)  * SCALE,
    rankTextFontSize:              (clientConfig.rankTextFontSize        ?? 60)  * SCALE,
    rankSpacing:                   (clientConfig.rankSpacing             ?? 140) * SCALE,
    rankPaddingY:                  (clientConfig.rankPaddingY            ?? 80)  * SCALE,
    rankYOffset:                   (clientConfig.rankYOffset             ?? 0)   * SCALE,
    rankNumX:                       45 * SCALE,
    rankTextX:                      (45 * SCALE) + ((clientConfig.rankFontSize ?? 60) * SCALE * 1.1),
    rankBoxWidth:                  (955 * SCALE) - ((45 * SCALE) + ((clientConfig.rankFontSize ?? 60) * SCALE * 1.1)),

    textOutlineWidth:               baseOutlineWidth * SCALE,

    // ── Fixed watermark geometry ──
    watermarkFontSize:              48 * SCALE,
    watermarkPadding:               20 * SCALE,
    creatorWatermarkFontSize:       44 * SCALE,
    creatorWatermarkBottomPadding:  80 * SCALE,
  };
}

/**
 * Merges client-provided config with system defaults, then attaches derived
 * geometry so every downstream function can use a single flat config object.
 */
function resolveLayoutConfig(clientConfig = {}) {
  // 1. Merge defaults with client overrides (client wins on every field)
  const merged = { ...DEFAULT_LAYOUT_CONFIG, ...clientConfig };

  // 2. Compute derived/scaled geometry and attach it
  const derived = getDerivedSettings(clientConfig);

  return { ...merged, ...derived };
}

// Map logical names to internal server paths
const FONT_MAP = {
  'Archivo Expanded Bold': '/usr/share/fonts/truetype/custom/Archivo-Expanded-Bold.ttf',
  'Arial Regular': '/usr/share/fonts/truetype/custom/Arial-Regular.ttf',
  'Rubik Bold': '/usr/share/fonts/truetype/custom/Rubik-Bold.ttf',
};

const emojiCache = new Map();

// Register available fonts
for (const [family, fontPath] of Object.entries(FONT_MAP)) {
  if (fs.existsSync(fontPath)) {
    registerFont(fontPath, { family });
    log.debug('Font registered', { family, fontPath });
  } else {
    log.warn('Font file not found, skipping', { family, fontPath });
  }
}

function getBaseFontFamily(config) {
  return config.fontFamily || 'Archivo Expanded Bold';
}

// ─────────────────────────────────────────────────────────────────────────────
// Status & Notification
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
    log.debug('Status updated', { postId, status, ...payload });
  } catch (e) {
    log.warn('Failed to update status file', { postId, error: e.message });
  }
}

async function notifyWebsite(postId, status, errorMessage = null, req = null) {
  let baseUrl = "https://ranktop.net";
  if (req && req.headers['x-callback-url']) {
    baseUrl = req.headers['x-callback-url'].replace(/\/$/, "");
  }
  const url = `${baseUrl}/api/internal/update-post`;
  try {
    log.info('Notifying website', { postId, status, url });
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-internal-secret': process.env.INTERNAL_SECRET
      },
      body: JSON.stringify({ postId, status, errorMessage })
    });
    if (!res.ok) {
      log.error('Webhook rejected', { postId, httpStatus: res.status });
    } else {
      log.info('Webhook acknowledged', { postId, status });
    }
  } catch (err) {
    log.error('Webhook notification failed', { postId, error: err.message });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Canvas Utilities
// ─────────────────────────────────────────────────────────────────────────────
function getEmojiUrl(emoji) {
  const codePoints = Array.from(emoji).map(c => c.codePointAt(0).toString(16)).join('-');
  return `https://cdn.jsdelivr.net/gh/jdecked/twemoji@15.0.3/assets/72x72/${codePoints}.png`;
}

function getFontForChar(char, config) {
  if (/\p{Extended_Pictographic}/u.test(char)) return 'Emoji';
  if (/[\u4e00-\u9fa5]|[\u3040-\u30ff]|[\uff00-\uffef]/.test(char)) return config.chineseFont;
  return getBaseFontFamily(config);
}

function segmentTextByFont(text, config) {
  const segments = [];
  if (!text) return segments;
  let currentSegment = { text: '', font: '' };
  for (const char of text) {
    const fontNeeded = getFontForChar(char, config);
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

function measureMixedText(ctx, text, fontSize, config) {
  const segments = segmentTextByFont(text, config);
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

async function drawMixedText(ctx, text, x, y, fontSize, fillStyle, strokeStyle, lineWidth, config) {
  const segments = segmentTextByFont(text, config);
  let currentX = x;
  const drawOutline = config.textShadow !== false;

  for (const s of segments) {
    if (s.font === 'Emoji') {
      const emojis = Array.from(s.text);
      for (const emoji of emojis) {
        try {
          const url = getEmojiUrl(emoji);
          let img = emojiCache.get(url);
          if (!img) {
            log.debug('Loading emoji', { emoji, url });
            img = await loadImage(url);
            emojiCache.set(url, img);
          }
          ctx.drawImage(img, currentX, y + (fontSize * 0.1), fontSize, fontSize);
        } catch (e) {
          log.warn('Emoji load failed', { emoji, error: e.message });
        }
        currentX += fontSize;
      }
    } else {
      ctx.font = `${fontSize}px "${s.font}"`;
      if (drawOutline && strokeStyle && lineWidth > 0) {
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

function buildWordColorMap(wordColors) {
  const map = new Map();
  for (const { word, color } of (wordColors || [])) {
    map.set(word.toLowerCase(), color);
  }
  return map;
}

async function drawColoredTitleLine(ctx, line, x, y, fontSize, config) {
  const wordColorMap = buildWordColorMap(config.titleWordColors);
  const strokeColor = 'black';

  ctx.shadowColor = config.titleShadowColor;
  ctx.shadowBlur = config.titleShadowBlur || 0;
  ctx.shadowOffsetX = 0; ctx.shadowOffsetY = 0;

  const words = line.split(' ');
  let currentX = x;

  for (let wi = 0; wi < words.length; wi++) {
    const word = words[wi];
    const accentColor = wordColorMap.get(word.toLowerCase());
    const displayWord = wi < words.length - 1 ? word + ' ' : word;

    const fill = (config.titleAccentOutline && accentColor) ? config.titleDefaultColor : (accentColor || config.titleDefaultColor);
    const stroke = (config.titleAccentOutline && accentColor) ? accentColor : strokeColor;

    await drawMixedText(ctx, displayWord, currentX, y, fontSize, fill, stroke, config.textOutlineWidth, config);
    currentX += measureMixedText(ctx, displayWord, fontSize, config);
  }
  ctx.shadowBlur = 0;
  ctx.shadowColor = 'rgba(0,0,0,0)';
}

function fitTextToBox(text, boxWidth, maxLines, initialFontSize, config) {
  const canvas = createCanvas(boxWidth, 100);
  const ctx = canvas.getContext('2d');
  for (let fontSize = initialFontSize; fontSize >= 1; fontSize -= 2) {
    const words = text.split(' '), lines = []; let currentLine = '';
    for (const word of words) {
      const test = currentLine ? `${currentLine} ${word}` : word;
      if (measureMixedText(ctx, test, fontSize, config) <= boxWidth) currentLine = test;
      else { lines.push(currentLine); currentLine = word; }
    }
    if (currentLine) lines.push(currentLine);
    if (lines.length <= maxLines) return { fontSize, lines };
  }
  return { fontSize: 10, lines: [text] };
}

function computeTitleBoxH(title, config) {
  const titleRes = fitTextToBox(title, config.titleBoxWidth, config.titleMaxLines, config.titleFontSize, config);
  const textH = (titleRes.lines.length * titleRes.fontSize) + ((titleRes.lines.length - 1) * config.titleLineSpacing);
  const subtitleH = config.subtitle ? config.subtitleTopMargin + config.subtitleFontSize : 0;
  const boxH = config.titleBoxTopPadding + textH + subtitleH + config.titleBoxBottomPadding;
  return { titleRes, boxH, textH, subtitleH };
}

async function drawTitleBlock(ctx, title, config) {
  const { titleRes, boxH, textH } = computeTitleBoxH(title, config);

  if (config.titleBackdrop === 'black' || config.titleBackdrop === 'white') {
    ctx.fillStyle = config.titleBackdrop;
    ctx.fillRect(0, 0, 1080, boxH);
  }

  const subtitleH = config.subtitle ? config.subtitleTopMargin + config.subtitleFontSize : 0;
  let currY = ((boxH - subtitleH) - textH) / 2;

  for (const line of titleRes.lines) {
    const lw = measureMixedText(ctx, line, titleRes.fontSize, config);
    await drawColoredTitleLine(ctx, line, (1080 - lw) / 2, currY, titleRes.fontSize, config);
    currY += titleRes.fontSize + config.titleLineSpacing;
  }

  if (config.subtitle) {
    const subW = measureMixedText(ctx, config.subtitle, config.subtitleFontSize, config);
    await drawMixedText(
      ctx, config.subtitle, (1080 - subW) / 2, currY + config.subtitleTopMargin,
      config.subtitleFontSize, config.subtitleColor, 'black', config.textOutlineWidth * 0.5, config
    );
  }

  return boxH;
}

async function drawWatermarks(ctx, config) {
  const fixedShadowColor = 'rgba(0,0,0,0.8)';
  const fixedShadowBlur = 15;

  const wmW = measureMixedText(ctx, config.watermarkText, config.watermarkFontSize, config);
  ctx.save();
  ctx.globalAlpha = config.watermarkOpacity;
  ctx.shadowColor = fixedShadowColor;
  ctx.shadowBlur = fixedShadowBlur;
  await drawMixedText(
    ctx, config.watermarkText,
    1080 - wmW - config.watermarkPadding,
    1920 - config.watermarkFontSize - config.watermarkPadding,
    config.watermarkFontSize, 'white', 'black', config.textOutlineWidth, config
  );
  ctx.restore();

  if (config.creatorWatermark) {
    const cwW = measureMixedText(ctx, config.creatorWatermark, config.creatorWatermarkFontSize, config);
    ctx.save();
    ctx.globalAlpha = config.creatorWatermarkOpacity;
    ctx.shadowColor = fixedShadowColor;
    ctx.shadowBlur = fixedShadowBlur;
    await drawMixedText(
      ctx, config.creatorWatermark, (1080 - cwW) / 2,
      1920 - config.creatorWatermarkFontSize - config.creatorWatermarkBottomPadding,
      config.creatorWatermarkFontSize, config.creatorWatermarkColor, 'black', config.textOutlineWidth * 0.6, config
    );
    ctx.restore();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Overlay Creation
// ─────────────────────────────────────────────────────────────────────────────
async function createTextOverlayImage(title, ranks, ranksToShow, config) {
  const canvas = createCanvas(1080, 1920), ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, 1080, 1920);
  ctx.textBaseline = 'top'; ctx.textAlign = 'left';

  const boxH = await drawTitleBlock(ctx, title, config);
  const drawOutline = config.textShadow !== false;

  for (let i = 0; i < ranksToShow; i++) {
    const idx = (ranks.length - ranksToShow) + i;
    // Y position calculated based on padding, title box height, and spacing between ranks
    const y = config.rankPaddingY + boxH + (idx * config.rankSpacing) + config.rankYOffset;

    // 1. CALCULATE RANK TEXT WRAPPING/FITTING
    const rRes = fitTextToBox(ranks[idx], config.rankBoxWidth, config.rankMaxLines, config.rankTextFontSize, config);

    const rankColor = config.rankColors[idx] || 'white';

    ctx.shadowColor = config.rankShadowColor;
    ctx.shadowBlur = config.rankShadowBlur || 0;
    ctx.font = `${config.rankFontSize}px "${getBaseFontFamily(config)}"`;

    // 2. DRAW RANK NUMBER (e.g., "1.", "10.")
    // Alignment: Right-aligned to config.rankTextX (minus a small gap)
    // This ensures that "1.", "10.", and "100." all align perfectly by the period.
    ctx.textAlign = 'right';
    const numAnchorX = config.rankTextX - (config.rankFontSize * 0.15);

    if (drawOutline) {
      ctx.strokeStyle = 'black'; ctx.lineWidth = config.textOutlineWidth;
      ctx.strokeText(`${idx + 1}.`, numAnchorX, y);
    }
    ctx.fillStyle = rankColor;
    ctx.fillText(`${idx + 1}.`, numAnchorX, y);
    ctx.shadowBlur = 0; ctx.shadowColor = 'rgba(0,0,0,0)';

    // Reset alignment for the rank text
    ctx.textAlign = 'left';

    const textColor = config.matchRankColor ? rankColor : 'white';

    // 3. VERTICAL ALIGNMENT LOGIC
    // Adjusts rank text Y position relative to the rank number's Y position
    // If the rank number is much larger, bottom align. Else center.
    const isMuchLarger = config.rankFontSize > config.rankTextFontSize * 1.8;
    const alignOffset = isMuchLarger
      ? (config.rankFontSize - (rRes.fontSize * 1.1))
      : (config.rankFontSize - rRes.fontSize) / 2;

    // 4. DRAW RANK TEXT (e.g., "The Title of the Item")
    await drawMixedText(
      ctx, rRes.lines[0], config.rankTextX, y + alignOffset,
      rRes.fontSize, textColor, 'black', config.textOutlineWidth, config
    );
  }

  await drawWatermarks(ctx, config);
  return canvas;
}

async function createBaseOverlayImage(title, config) {
  const canvas = createCanvas(1080, 1920), ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, 1080, 1920);
  ctx.textBaseline = 'top'; ctx.textAlign = 'left';

  await drawTitleBlock(ctx, title, config);
  await drawWatermarks(ctx, config);
  return canvas;
}

async function createRankOverlayImage(ranks, rankIndex, boxH, config) {
  const canvas = createCanvas(1080, 1920), ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, 1080, 1920);
  ctx.textBaseline = 'top'; ctx.textAlign = 'left';

  const y = config.rankPaddingY + boxH + (rankIndex * config.rankSpacing) + config.rankYOffset;

  // 1. CALCULATE RANK TEXT WRAPPING/FITTING
  const rRes = fitTextToBox(ranks[rankIndex], config.rankBoxWidth, config.rankMaxLines, config.rankTextFontSize, config);
  const rankColor = config.rankColors[rankIndex] || 'white';
  const drawOutline = config.textShadow !== false;

  ctx.shadowColor = config.rankShadowColor;
  ctx.shadowBlur = config.rankShadowBlur || 0;
  ctx.font = `${config.rankFontSize}px "${getBaseFontFamily(config)}"`;

  // 2. DRAW RANK NUMBER (e.g., "1.", "10.")
  // Alignment: Right-aligned to config.rankTextX (minus a small gap)
  // This ensures that "1.", "10.", and "100." all align perfectly by the period.
  ctx.textAlign = 'right';
  const numAnchorX = config.rankTextX - (config.rankFontSize * 0.15);

  if (drawOutline) {
    ctx.strokeStyle = 'black'; ctx.lineWidth = config.textOutlineWidth;
    ctx.strokeText(`${rankIndex + 1}.`, numAnchorX, y);
  }
  ctx.fillStyle = rankColor;
  ctx.fillText(`${rankIndex + 1}.`, numAnchorX, y);

  ctx.shadowBlur = 0; ctx.shadowColor = 'rgba(0,0,0,0)';

  // Reset alignment for the rank text
  ctx.textAlign = 'left';

  const textColor = config.matchRankColor ? rankColor : 'white';

  // 3. VERTICAL ALIGNMENT LOGIC
  // Adjusts rank text Y position relative to the rank number's Y position
  // If the rank number is much larger, bottom align. Else center.
  const isMuchLarger = config.rankFontSize > config.rankTextFontSize * 1.8;
  const alignOffset = isMuchLarger
    ? (config.rankFontSize - (rRes.fontSize * 1.1))
    : (config.rankFontSize - rRes.fontSize) / 2;

  // 4. DRAW RANK TEXT (e.g., "The Title of the Item")
  await drawMixedText(
    ctx, rRes.lines[0], config.rankTextX, y + alignOffset,
    rRes.fontSize, textColor, 'black', config.textOutlineWidth, config
  );
  return canvas;
}

// ─────────────────────────────────────────────────────────────────────────────
// FFmpeg Helpers
// ─────────────────────────────────────────────────────────────────────────────
function spawnWithTimeout(cmd, args, ms, label = 'Process') {
  return new Promise((resolve, reject) => {
    const proc = spawn(cmd, args);
    let stderr = '';
    let lastProgressLog = Date.now();

    proc.stderr.on('data', d => {
      stderr += d.toString();
      // FFmpeg writes progress to stderr. Log a heartbeat every 10s so we know it's alive.
      const now = Date.now();
      if (now - lastProgressLog > 10000) {
        // Extract the last non-empty line from stderr for a concise heartbeat
        const lines = stderr.split('\n').filter(l => l.trim());
        const lastLine = lines[lines.length - 1] || '';
        log.debug(`${label} heartbeat`, { lastLine: lastLine.slice(0, 120) });
        lastProgressLog = now;
      }
    });

    proc.on('error', (err) => {
      log.error(`${label} spawn error`, { error: err.message });
      reject(err);
    });

    proc.on('close', code => {
      clearTimeout(timer);
      if (code === 0) {
        resolve();
      } else {
        const tail = stderr.slice(-600);
        log.error(`${label} failed`, { exitCode: code, stderrTail: tail });
        reject(new Error(`${label} failed (${code}): ${tail}`));
      }
    });

    const timer = setTimeout(() => {
      proc.kill('SIGKILL');
      const tail = stderr.slice(-400);
      log.error(`${label} timed out`, { timeoutMs: ms, stderrTail: tail });
      reject(new Error(`${label} timed out after ${ms / 1000}s`));
    }, ms);
  });
}

function downloadWithTimeout(gcsFile, destination, ms, label = 'Download') {
  return Promise.race([
    gcsFile.download({ destination }).then(r => { log.timeEnd(label); return r; }),
    new Promise((_, reject) =>
      setTimeout(() => {
        log.error(`${label} timed out`, { source: gcsFile.name, timeoutMs: ms });
        reject(new Error(`${label} timed out after ${ms / 1000}s`));
      }, ms)
    )
  ]);
}

function applyOverlay(inputPath, overlayPath, outputPath, boxH = 0, config) {
  let filter;
  const videoY = config.pushVideoDown ? Math.ceil(boxH) : 0;

  if (config.titleBackdrop === 'blurred' && boxH > 0) {
    filter = [
      `[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[v_fit]`,
      `[v_fit]split[v_main][forblur]`,
      `[forblur]crop=1080:${Math.ceil(boxH)}:0:0,boxblur=20:5[blurred_top]`,
      `color=black:s=1080x1920[bg]`,
      `[bg][v_main]overlay=0:${videoY}[v_with_video]`,
      `[v_with_video][blurred_top]overlay=0:0[with_blur]`,
      `[1:v]scale=1080:1920[ov]`,
      `[with_blur][ov]overlay=0:0`,
    ].join(';');
  } else {
    filter = [
      `[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[v_fit]`,
      `color=black:s=1080x1920[bg]`,
      `[bg][v_fit]overlay=0:${videoY}[v_with_video]`,
      `[1:v]scale=1080:1920[ov]`,
      `[v_with_video][ov]overlay=0:0`,
    ].join(';');
  }
  return spawnWithTimeout('ffmpeg', [
    '-i', inputPath, '-loop', '1', '-i', overlayPath, '-filter_complex', filter,
    '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23', '-c:a', 'aac',
    '-shortest',
    '-movflags', '+faststart', '-y', outputPath
  ], 300000, 'Overlay');
}

async function generateThumbnail(videoPath, outputPath) {
  return spawnWithTimeout('ffmpeg', [
    '-i', videoPath, '-ss', '00:00:01', '-vframes', '1', '-q:v', '2', '-y', outputPath
  ], 30000, 'Thumbnail');
}

function stitchClips(listPath, outputPath) {
  return spawnWithTimeout('ffmpeg', [
    '-f', 'concat', '-safe', '0', '-i', listPath,
    '-c', 'copy', '-movflags', '+faststart', '-y', outputPath
  ], 120000, 'Stitch');
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipelines
// ─────────────────────────────────────────────────────────────────────────────
async function processAutoStitch(req, res, { postId, title, ranks, filePaths, config }) {
  const tempFiles = [];
  try {
    const totalClips = filePaths.length;
    log.info('AutoStitch job started', { postId, title, totalClips, rankCount: ranks.length });

    await updateStatusFile(postId, 'PROCESSING', { progress: 5 });
    const parsedRanks = typeof ranks === 'string' ? JSON.parse(ranks) : ranks;

    log.info('Downloading source clips', { totalClips });
    const local = await Promise.all(filePaths.map(async (fp, i) => {
      const p = `/tmp/in_${i}_${uuidv4()}${path.extname(fp) || '.mp4'}`;
      await downloadWithTimeout(cacheBucket.file(fp), p, 120000, `Download clip ${i + 1}/${totalClips}`);
      tempFiles.push(p);
      return p;
    }));

    const { boxH } = computeTitleBoxH(title, config);

    const processed = [];

    log.info('Applying overlays to clips...');
    for (let i = 0; i < local.length; i++) {
      // Progress: 10% → 75% across all clips (extra headroom for 10 clips)
      const prog = 10 + Math.floor(((i + 1) / totalClips) * 65);
      const ovPath = `/tmp/ov_${i}_${uuidv4()}.png`;
      const outPath = `/tmp/proc_${i}_${uuidv4()}.mp4`;
      tempFiles.push(ovPath, outPath);

      const canvas = await createTextOverlayImage(title, parsedRanks, i + 1, config);
      fs.writeFileSync(ovPath, canvas.toBuffer('image/png'));

      await applyOverlay(local[i], ovPath, outPath, boxH, config);

      const stat = fs.statSync(outPath);
      processed.push(outPath);

      await updateStatusFile(postId, 'PROCESSING', { progress: prog });
    }

    log.info('All clips processed, stitching', { totalClips });
    await updateStatusFile(postId, 'PROCESSING', { progress: 80 });

    const listPath = `/tmp/l_${uuidv4()}.txt`;
    const finalPath = `/tmp/f_${uuidv4()}.mp4`;
    tempFiles.push(listPath, finalPath);
    fs.writeFileSync(listPath, processed.map(p => `file '${p}'`).join('\n'));
    await stitchClips(listPath, finalPath);

    const stitchedStat = fs.statSync(finalPath);
    log.info('Stitch complete', { outputSizeMB: (stitchedStat.size / 1024 / 1024).toFixed(1) });

    await updateStatusFile(postId, 'PROCESSING', { progress: 90 });

    const thumbPath = `/tmp/t_${uuidv4()}.jpg`;
    tempFiles.push(thumbPath);
    log.info('Generating thumbnail');
    await generateThumbnail(finalPath, thumbPath);

    log.info('Uploading final outputs to GCS', { postId });
    await Promise.all([
      outputBucket.upload(finalPath, { destination: `${postId}.mp4` }),
      thumbnailBucket.upload(thumbPath, { destination: `${postId}.jpg`, metadata: { contentType: 'image/jpeg' } })
    ]);
    log.info('Upload complete', { postId });

    await updateStatusFile(postId, 'READY', { progress: 100 });
    await notifyWebsite(postId, 'READY', null, req);
    log.info('AutoStitch job finished successfully', { postId });
    res.status(200).json({ status: 'SUCCESS' });
  } catch (error) {
    log.error('AutoStitch job failed', { postId, error: error.message, stack: error.stack?.split('\n').slice(0, 4).join(' | ') });
    await updateStatusFile(postId, 'FAILED', { error: error.message });
    await notifyWebsite(postId, 'FAILED', error.message, req);
    res.status(500).json({ error: error.message });
  } finally {
    let cleaned = 0;
    tempFiles.forEach(f => { try { fs.unlinkSync(f); cleaned++; } catch {} });
    log.info('Temp files cleaned up', { count: cleaned });
    emojiCache.clear();
  }
}

async function processPreEdited(req, res, { postId, title, ranks, filePath, timestamps, endTime, config }) {
  const tempFiles = [];
  try {
    log.info('PreEdited job started', { postId, title, rankCount: ranks.length, timestampCount: timestamps.length });
    await updateStatusFile(postId, 'PROCESSING', { progress: 5 });

    const parsedRanks = typeof ranks === 'string' ? JSON.parse(ranks) : ranks;
    const parsedEndTime = typeof endTime === 'string' ? parseFloat(endTime) : endTime;
    log.info('Job params', { rankCount: parsedRanks.length, endTime: parsedEndTime });

    const sourcePath = `/tmp/source_${uuidv4()}${path.extname(filePath) || '.mp4'}`;
    await downloadWithTimeout(cacheBucket.file(filePath), sourcePath, 120000, 'Download source');
    const sourceStat = fs.statSync(sourcePath);
    log.info('Source downloaded', { sizeKB: Math.round(sourceStat.size / 1024) });
    tempFiles.push(sourcePath);

    const { boxH } = computeTitleBoxH(title, config);
    log.info('Title box computed', { boxH: Math.round(boxH) });

    const basePath = `/tmp/base_${uuidv4()}.png`;
    tempFiles.push(basePath);
    log.debug('Rendering base overlay');
    const baseCanvas = await createBaseOverlayImage(title, config);
    fs.writeFileSync(basePath, baseCanvas.toBuffer('image/png'));

    const sortedTimestamps = [...timestamps].sort((a, b) => a.time - b.time);
    log.info('Timestamps sorted', { timestamps: sortedTimestamps.map(t => t.time) });

    const rankPaths = [];

    for (let i = 0; i < parsedRanks.length; i++) {
      const prog = 25 + Math.floor(((i + 1) / parsedRanks.length) * 35);
      const rankIndex = parsedRanks.length - 1 - i;
      log.info('Rendering rank overlay', { slot: i + 1, rankIndex: rankIndex + 1, rankText: parsedRanks[rankIndex]?.slice(0, 40), progress: `${prog}%` });
      await updateStatusFile(postId, 'PROCESSING', { progress: prog });

      const rankPath = `/tmp/rank_${i}_${uuidv4()}.png`;
      tempFiles.push(rankPath);

      const rankCanvas = await createRankOverlayImage(parsedRanks, rankIndex, boxH, config);
      fs.writeFileSync(rankPath, rankCanvas.toBuffer('image/png'));
      rankPaths.push({ path: rankPath, rankIndex, timestampSlot: i });
    }

    log.info('All rank overlays rendered, building FFmpeg filter', { rankCount: rankPaths.length });
    await updateStatusFile(postId, 'PROCESSING', { progress: 65 });

    const finalPath = `/tmp/f_${uuidv4()}.mp4`;
    tempFiles.push(finalPath);
    const inputArgs = ['-i', sourcePath, '-loop', '1', '-i', basePath];
    for (const { path } of rankPaths) inputArgs.push('-loop', '1', '-i', path);

    const filterParts = [];
    let scaledLabel;

    const videoY = config.pushVideoDown ? Math.ceil(boxH) : 0;

    if (config.titleBackdrop === 'blurred') {
      filterParts.push(
        `[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[v_fit]`,
        `[v_fit]split[v_main][forblur]`,
        `[forblur]crop=1080:${Math.ceil(boxH)}:0:0,boxblur=20:5[blurred_top]`,
        `color=black:s=1080x1920[bg]`,
        `[bg][v_main]overlay=0:${videoY}[v_with_video]`,
        `[v_with_video][blurred_top]overlay=0:0[v_preblur]`
      );
      scaledLabel = 'v_preblur';
    } else {
      filterParts.push(
        `[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[v_fit]`,
        `color=black:s=1080x1920[bg]`,
        `[bg][v_fit]overlay=0:${videoY}[v_scaled]`
      );
      scaledLabel = 'v_scaled';
    }
    filterParts.push(`[1:v]scale=1080:1920[base_ov]`, `[${scaledLabel}][base_ov]overlay=0:0[v_base]`);

    let prevLabel = 'v_base';
    for (let i = 0; i < rankPaths.length; i++) {
      const { timestampSlot } = rankPaths[i];
      const start = sortedTimestamps[timestampSlot]?.time ?? 0;
      filterParts.push(`[${i + 2}:v]scale=1080:1920[r${i}]`);
      filterParts.push(`[${prevLabel}][r${i}]overlay=0:0:enable='between(t,${start},${parsedEndTime})'[v${i}]`);
      prevLabel = `v${i}`;
    }

    log.info('Spawning FFmpeg pre-edited overlay', { inputCount: inputArgs.filter(a => a === '-i').length, filterPartCount: filterParts.length });

    await spawnWithTimeout('ffmpeg', [
      ...inputArgs, '-filter_complex', filterParts.join(';'),
      '-map', `[${prevLabel}]`, '-map', '0:a?',
      '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23', '-c:a', 'aac', 
      '-shortest',
      '-movflags', '+faststart', '-y', finalPath
    ], 600000, 'Pre-edited overlay');

    const finalStat = fs.statSync(finalPath);
    log.info('Pre-edited render complete', { outputSizeMB: (finalStat.size / 1024 / 1024).toFixed(1) });

    const thumbPath = `/tmp/t_${uuidv4()}.jpg`;
    tempFiles.push(thumbPath);
    log.info('Generating thumbnail');
    await generateThumbnail(finalPath, thumbPath);

    log.info('Uploading final outputs to GCS', { postId });
    await Promise.all([
      outputBucket.upload(finalPath, { destination: `${postId}.mp4` }),
      thumbnailBucket.upload(thumbPath, { destination: `${postId}.jpg`, metadata: { contentType: 'image/jpeg' } })
    ]);
    log.info('Upload complete', { postId });

    await updateStatusFile(postId, 'READY', { progress: 100 });
    await notifyWebsite(postId, 'READY', null, req);
    log.info('PreEdited job finished successfully', { postId });
    res.status(200).json({ status: 'SUCCESS' });
  } catch (error) {
    log.error('PreEdited job failed', { postId, error: error.message, stack: error.stack?.split('\n').slice(0, 4).join(' | ') });
    await updateStatusFile(postId, 'FAILED', { error: error.message });
    await notifyWebsite(postId, 'FAILED', error.message, req);
    res.status(500).json({ error: error.message });
  } finally {
    let cleaned = 0;
    tempFiles.forEach(f => { try { fs.unlinkSync(f); cleaned++; } catch {} });
    log.info('Temp files cleaned up', { count: cleaned });
    emojiCache.clear();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Main Handler
// ─────────────────────────────────────────────────────────────────────────────
functions.http('processVideos', async (req, res) => {
  const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;

  const {
    action, videoCount, sessionId, fileTypes, fileType,
    title, ranks, filePaths, filePath, timestamps, endTime,
    postId, videoMode, layoutConfig: rawClientConfig
  } = body;

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

  log.info('Request received', { action: action || videoMode, postId, sessionId, videoCount });

  let clientConfig = rawClientConfig;
  if (typeof clientConfig === 'string') {
    try { clientConfig = JSON.parse(clientConfig); } catch (e) { clientConfig = {}; }
  }
  const activeConfig = resolveLayoutConfig(clientConfig || {});

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
    log.info('Upload URLs generated', { sessionId, count: uploadUrls.length });
    return res.json({ uploadUrls, filePaths: generatedPaths, sessionId });
  }

  if (action === 'getUploadUrl') {
    if (!sessionId || !fileType) return res.status(400).json({ error: "Missing sessionId or fileType" });
    const ext = fileType.split('/')[1] || 'mp4';
    const fileName = `${sessionId}/pre_source.${ext}`;
    const [url] = await cacheBucket.file(fileName).getSignedUrl({
      version: 'v4', action: 'write', expires: Date.now() + 900000, contentType: fileType
    });
    log.info('Single upload URL generated', { sessionId, fileName });
    return res.json({ uploadUrl: url, filePath: fileName });
  }

  if (videoMode === 'pre-edited') {
    const parsedTimestamps = typeof timestamps === 'string' ? JSON.parse(timestamps) : timestamps;

    if (!filePath || !postId || !parsedTimestamps || !Array.isArray(parsedTimestamps)) {
      log.error('Missing required pre-edited params', { hasFilePath: !!filePath, hasPostId: !!postId, hasTimestamps: !!parsedTimestamps });
      return res.status(400).json({ error: "Missing filePath, postId, or timestamps" });
    }
    return processPreEdited(req, res, {
      postId, title, ranks, filePath,
      timestamps: parsedTimestamps,
      endTime, config: activeConfig
    });
  } else {
    if (!filePaths || !postId) {
      log.error('Missing required auto-stitch params', { hasFilePaths: !!filePaths, hasPostId: !!postId });
      return res.status(400).json({ error: "Missing filePaths or postId" });
    }
    return processAutoStitch(req, res, { postId, title, ranks, filePaths, config: activeConfig });
  }
});
