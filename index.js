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
// LAYOUT CONFIG
// ─────────────────────────────────────────────────────────────────────────────
const DEFAULT_LAYOUT_CONFIG = {
  fontFamily: 'Archivo Expanded Bold',
  chineseFont: 'Noto Sans CJK SC',
  rankColors: ['#FFD700', '#C0C0C0', '#CD7F32', 'white', 'white'],
  titleFontSize: 100,
  titleLineSpacing: 30,
  titleBoxWidth: 980,
  titleMaxLines: 2,
  titleBoxTopPadding: 30,
  titleBoxBottomPadding: 40,
  titleBackdrop: 'black', // 'none', 'black', 'white', 'blurred'
  titleWordColors: [],
  titleDefaultColor: 'white',
  titleShadowBlur: 25,
  titleShadowColor: 'rgba(0,0,0,0.8)',
  rankShadowBlur: 5,
  rankShadowColor: 'rgba(0,0,0,0.8)',

  subtitle: '',
  subtitleFontSize: 44,
  subtitleColor: '#CCCCCC',
  subtitleTopMargin: 10,
  rankFontSize: 60,
  rankSpacing: 140, 
  rankPaddingY: 80,
  rankNumX: 45,
  rankTextX: 125,
  rankBoxWidth: 830,
  rankMaxLines: 1,
  textOutlineWidth: 18,
  textShadow: true, // Text outline toggle

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
 * Merges client-provided config with system defaults.
 */
function resolveLayoutConfig(clientConfig = {}) {
  return { ...DEFAULT_LAYOUT_CONFIG, ...clientConfig };
}

// Map logical names to internal server paths
const FONT_MAP = {
  'Archivo Expanded Bold': '/usr/share/fonts/truetype/custom/Archivo-Expanded-Bold.ttf',
  'Arial Regular': '/usr/share/fonts/truetype/custom/Arial-Regular.ttf'
};

const emojiCache = new Map();

// Register available fonts
for (const [family, fontPath] of Object.entries(FONT_MAP)) {
  if (fs.existsSync(fontPath)) {
    registerFont(fontPath, { family });
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
  } catch (e) {
    console.warn("Failed to update status file:", e.message);
  }
}

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
          if (!img) { img = await loadImage(url); emojiCache.set(url, img); }
          ctx.drawImage(img, currentX, y + (fontSize * 0.1), fontSize, fontSize);
        } catch (e) { console.warn(`Emoji Load Failed: ${emoji}`); }
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
    const color = wordColorMap.get(word.toLowerCase()) || config.titleDefaultColor;
    const displayWord = wi < words.length - 1 ? word + ' ' : word;
    await drawMixedText(ctx, displayWord, currentX, y, fontSize, color, strokeColor, config.textOutlineWidth, config);
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
    const y = config.rankPaddingY + boxH + (idx * config.rankSpacing);
    const rRes = fitTextToBox(ranks[idx], config.rankBoxWidth, config.rankMaxLines, config.rankFontSize, config);

    const rankColor = config.rankColors[idx] || 'white';

    ctx.shadowColor = config.rankShadowColor;
    ctx.shadowBlur = config.rankShadowBlur || 0;
    ctx.font = `${config.rankFontSize}px "${getBaseFontFamily(config)}"`;
    
    if (drawOutline) {
      ctx.strokeStyle = 'black'; ctx.lineWidth = config.textOutlineWidth;
      ctx.strokeText(`${idx + 1}.`, config.rankNumX, y);
    }
    ctx.fillStyle = rankColor;
    ctx.fillText(`${idx + 1}.`, config.rankNumX, y);
    ctx.shadowBlur = 0; ctx.shadowColor = 'rgba(0,0,0,0)';

    const textColor = config.matchRankColor ? rankColor : 'white';

    await drawMixedText(
      ctx, rRes.lines[0], config.rankTextX, y + ((config.rankFontSize - rRes.fontSize) / 2), 
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

  const y = config.rankPaddingY + boxH + (rankIndex * config.rankSpacing);
  const rRes = fitTextToBox(ranks[rankIndex], config.rankBoxWidth, config.rankMaxLines, config.rankFontSize, config);
  const rankColor = config.rankColors[rankIndex] || 'white';
  const drawOutline = config.textShadow !== false;

  ctx.shadowColor = config.rankShadowColor;
  ctx.shadowBlur = config.rankShadowBlur || 0;
  ctx.font = `${config.rankFontSize}px "${getBaseFontFamily(config)}"`;

  if (drawOutline) {
    ctx.strokeStyle = 'black'; ctx.lineWidth = config.textOutlineWidth;
    ctx.strokeText(`${rankIndex + 1}.`, config.rankNumX, y);
  }
  ctx.fillStyle = rankColor;
  ctx.fillText(`${rankIndex + 1}.`, config.rankNumX, y);
  
  ctx.shadowBlur = 0; ctx.shadowColor = 'rgba(0,0,0,0)';

  const textColor = config.matchRankColor ? rankColor : 'white';

  await drawMixedText(
    ctx, rRes.lines[0], config.rankTextX, y + ((config.rankFontSize - rRes.fontSize) / 2), 
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

function applyOverlay(inputPath, overlayPath, outputPath, boxH = 0, config) {
  let filter;
  if (config.titleBackdrop === 'blurred' && boxH > 0) {
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
    '-i', inputPath, '-i', overlayPath, '-filter_complex', filter,
    '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23', '-c:a', 'aac',
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
    await updateStatusFile(postId, 'PROCESSING', { progress: 5 });
    const parsedRanks = typeof ranks === 'string' ? JSON.parse(ranks) : ranks;
    const local = await Promise.all(filePaths.map(async (fp, i) => {
      const p = `/tmp/in_${i}_${uuidv4()}${path.extname(fp) || '.mp4'}`;
      await downloadWithTimeout(cacheBucket.file(fp), p, 120000, `Download clip ${i}`);
      tempFiles.push(p);
      return p;
    }));

    const { boxH } = computeTitleBoxH(title, config);
    const processed = [];

    for (let i = 0; i < local.length; i++) {
      const prog = 10 + Math.floor((i / local.length) * 60);
      await updateStatusFile(postId, 'PROCESSING', { progress: prog });

      const ovPath = `/tmp/ov_${i}_${uuidv4()}.png`, outPath = `/tmp/proc_${i}_${uuidv4()}.mp4`;
      tempFiles.push(ovPath, outPath);

      const canvas = await createTextOverlayImage(title, parsedRanks, i + 1, config);
      fs.writeFileSync(ovPath, canvas.toBuffer('image/png'));
      await applyOverlay(local[i], ovPath, outPath, boxH, config);
      processed.push(outPath);
    }

    await updateStatusFile(postId, 'PROCESSING', { progress: 80 });
    const listPath = `/tmp/l_${uuidv4()}.txt`, finalPath = `/tmp/f_${uuidv4()}.mp4`;
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

async function processPreEdited(req, res, { postId, title, ranks, filePath, timestamps, endTime, config }) {
  const tempFiles = [];
  try {
    await updateStatusFile(postId, 'PROCESSING', { progress: 5 });
    
    const parsedRanks = typeof ranks === 'string' ? JSON.parse(ranks) : ranks;
    const parsedEndTime = typeof endTime === 'string' ? parseFloat(endTime) : endTime;

    const sourcePath = `/tmp/source_${uuidv4()}${path.extname(filePath) || '.mp4'}`;
    await downloadWithTimeout(cacheBucket.file(filePath), sourcePath, 120000, 'Download source');
    tempFiles.push(sourcePath);

    const { boxH } = computeTitleBoxH(title, config);
    
    const basePath = `/tmp/base_${uuidv4()}.png`;
    tempFiles.push(basePath);
    const baseCanvas = await createBaseOverlayImage(title, config);
    fs.writeFileSync(basePath, baseCanvas.toBuffer('image/png'));

    const sortedTimestamps = [...timestamps].sort((a, b) => a.time - b.time);
    const rankPaths = [];

    for (let i = 0; i < parsedRanks.length; i++) {
      const prog = 25 + Math.floor((i / parsedRanks.length) * 35);
      await updateStatusFile(postId, 'PROCESSING', { progress: prog });
      
      const rankPath = `/tmp/rank_${i}_${uuidv4()}.png`;
      tempFiles.push(rankPath);
      
      const rankIndex = parsedRanks.length - 1 - i;
      
      const rankCanvas = await createRankOverlayImage(parsedRanks, rankIndex, boxH, config);
      fs.writeFileSync(rankPath, rankCanvas.toBuffer('image/png'));
      rankPaths.push({ path: rankPath, rankIndex, timestampSlot: i });
    }

    const finalPath = `/tmp/f_${uuidv4()}.mp4`;
    tempFiles.push(finalPath);
    const inputArgs = ['-i', sourcePath, '-i', basePath];
    for (const { path } of rankPaths) inputArgs.push('-i', path);

    const filterParts = [];
    let scaledLabel;
    
    if (config.titleBackdrop === 'blurred') {
      filterParts.push(
        `[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[scaled]`,
        `[scaled]split[full][forblur]`,
        `[forblur]crop=1080:${Math.ceil(boxH)}:0:0,boxblur=20:5[blurred_top]`,
        `[full][blurred_top]overlay=0:0[v_preblur]`
      );
      scaledLabel = 'v_preblur';
    } else {
      filterParts.push(`[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[v_scaled]`);
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

    await spawnWithTimeout('ffmpeg', [
      ...inputArgs, '-filter_complex', filterParts.join(';'),
      '-map', `[${prevLabel}]`, '-map', '0:a?',
      '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23', '-c:a', 'aac', '-movflags', '+faststart', '-y', finalPath
    ], 600000, 'Pre-edited overlay');

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
// Main Handler
// ─────────────────────────────────────────────────────────────────────────────
functions.http('processVideos', async (req, res) => {
  const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;

  const { action, videoCount, sessionId, fileTypes, fileType, title, ranks, filePaths, filePath, timestamps, endTime, postId, videoMode, layoutConfig: rawClientConfig } = body;

  let clientConfig = rawClientConfig;
  if (typeof clientConfig === 'string') {
    try {
      clientConfig = JSON.parse(clientConfig);
    } catch (e) {
      console.error("Failed to parse layoutConfig string:", e);
      clientConfig = {};
    }
  }

  const activeConfig = resolveLayoutConfig(clientConfig || {});

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

  if (action === 'getUploadUrl') {
    if (!sessionId || !fileType) return res.status(400).json({ error: "Missing sessionId or fileType" });
    const ext = fileType.split('/')[1] || 'mp4';
    const fileName = `${sessionId}/pre_source.${ext}`;
    const [url] = await cacheBucket.file(fileName).getSignedUrl({
      version: 'v4', action: 'write', expires: Date.now() + 900000, contentType: fileType
    });
    return res.json({ uploadUrl: url, filePath: fileName });
  }

  if (videoMode === 'pre-edited') {
    const parsedTimestamps = typeof timestamps === 'string' ? JSON.parse(timestamps) : timestamps;
    
    if (!filePath || !postId || !parsedTimestamps || !Array.isArray(parsedTimestamps)) {
      return res.status(400).json({ error: "Missing filePath, postId, or timestamps" });
    }
    return processPreEdited(req, res, { 
      postId, title, ranks, filePath, 
      timestamps: parsedTimestamps, 
      endTime, config: activeConfig 
    });
  } else {
    if (!filePaths || !postId) return res.status(400).json({ error: "Missing filePaths or postId" });
    return processAutoStitch(req, res, { postId, title, ranks, filePaths, config: activeConfig });
  }
});
