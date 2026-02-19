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

const LAYOUT_CONFIG = {
  fontPath: '/usr/share/fonts/truetype/custom/font.ttf',
  chineseFont: 'Noto Sans CJK SC',
  rankColors: ['#FFD700', '#C0C0C0', '#CD7F32', 'white', 'white'],
  titleFontSize: 100, titleLineSpacing: 30, titleBoxWidth: 980,
  titleMaxLines: 2, titleBoxTopPadding: 30, titleBoxBottomPadding: 40,
  rankFontSize: 60, rankSpacing: 140, rankPaddingY: 80, rankNumX: 45,
  rankTextX: 125, rankBoxWidth: 830, rankMaxLines: 1,
  watermarkText: 'ranktop.net', watermarkFontSize: 48, watermarkPadding: 20, watermarkOpacity: 0.6,
  textOutlineWidth: 12
};

const emojiCache = new Map();
if (fs.existsSync(LAYOUT_CONFIG.fontPath)) {
  registerFont(LAYOUT_CONFIG.fontPath, { family: 'CustomFont' });
}

// --- Status Management ---
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

// --- Database Webhook ---
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

// --- Utilities ---
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
          if (!img) {
            img = await loadImage(url);
            emojiCache.set(url, img);
          }
          ctx.drawImage(img, currentX, y + (fontSize * 0.1), fontSize, fontSize);
        } catch (e) { console.warn(`Emoji Load Failed: ${emoji}`); }
        currentX += fontSize;
      }
    } else {
      ctx.font = `${fontSize}px "${s.font}"`;
      if (strokeStyle && lineWidth > 0) {
        ctx.strokeStyle = strokeStyle; ctx.lineWidth = lineWidth;
        ctx.strokeText(s.text, currentX, y);
      }
      ctx.fillStyle = fillStyle;
      ctx.fillText(s.text, currentX, y);
      currentX += ctx.measureText(s.text).width;
    }
  }
}

// Used by the auto-stitch pipeline — renders title, all revealed ranks so far, and watermark.
async function createTextOverlayImage(title, ranks, ranksToShow) {
  const canvas = createCanvas(1080, 1920), ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, 1080, 1920);
  ctx.textBaseline = 'top'; ctx.textAlign = 'left';

  const titleRes = fitTextToBox(title, LAYOUT_CONFIG.titleBoxWidth, LAYOUT_CONFIG.titleMaxLines, LAYOUT_CONFIG.titleFontSize);
  const textH = (titleRes.lines.length * titleRes.fontSize) + ((titleRes.lines.length - 1) * LAYOUT_CONFIG.titleLineSpacing);
  const boxH = LAYOUT_CONFIG.titleBoxTopPadding + textH + LAYOUT_CONFIG.titleBoxBottomPadding;

  ctx.fillStyle = 'black'; ctx.fillRect(0, 0, 1080, boxH);
  let currY = (boxH - textH) / 2;
  for (const line of titleRes.lines) {
    const lw = measureMixedText(ctx, line, titleRes.fontSize);
    await drawMixedText(ctx, line, (1080 - lw) / 2, currY, titleRes.fontSize, 'white');
    currY += titleRes.fontSize + LAYOUT_CONFIG.titleLineSpacing;
  }

  for (let i = 0; i < ranksToShow; i++) {
    const idx = (ranks.length - ranksToShow) + i;
    const y = LAYOUT_CONFIG.rankPaddingY + boxH + (idx * LAYOUT_CONFIG.rankSpacing);
    const rRes = fitTextToBox(ranks[idx], LAYOUT_CONFIG.rankBoxWidth, LAYOUT_CONFIG.rankMaxLines, LAYOUT_CONFIG.rankFontSize);

    ctx.font = `${LAYOUT_CONFIG.rankFontSize}px "CustomFont"`;
    ctx.strokeStyle = 'black'; ctx.lineWidth = LAYOUT_CONFIG.textOutlineWidth;
    ctx.strokeText(`${idx + 1}.`, LAYOUT_CONFIG.rankNumX, y);
    ctx.fillStyle = LAYOUT_CONFIG.rankColors[idx] || 'white';
    ctx.fillText(`${idx + 1}.`, LAYOUT_CONFIG.rankNumX, y);

    await drawMixedText(ctx, rRes.lines[0], LAYOUT_CONFIG.rankTextX, y + ((LAYOUT_CONFIG.rankFontSize - rRes.fontSize) / 2), rRes.fontSize, 'white', 'black', LAYOUT_CONFIG.textOutlineWidth);
  }

  const wmW = measureMixedText(ctx, LAYOUT_CONFIG.watermarkText, LAYOUT_CONFIG.watermarkFontSize);
  ctx.save();
  ctx.globalAlpha = LAYOUT_CONFIG.watermarkOpacity;
  await drawMixedText(ctx, LAYOUT_CONFIG.watermarkText, 1080 - wmW - LAYOUT_CONFIG.watermarkPadding, 1920 - LAYOUT_CONFIG.watermarkFontSize - LAYOUT_CONFIG.watermarkPadding, LAYOUT_CONFIG.watermarkFontSize, 'white', 'black', LAYOUT_CONFIG.textOutlineWidth);
  ctx.restore();

  return canvas;
}

// --- Pre-edited overlay helpers ---

// Shared helper: computes title box height so rank positioning is consistent
// between the base overlay and the per-rank overlays.
function computeTitleBoxH(title) {
  const titleRes = fitTextToBox(title, LAYOUT_CONFIG.titleBoxWidth, LAYOUT_CONFIG.titleMaxLines, LAYOUT_CONFIG.titleFontSize);
  const textH = (titleRes.lines.length * titleRes.fontSize) + ((titleRes.lines.length - 1) * LAYOUT_CONFIG.titleLineSpacing);
  return { titleRes, boxH: LAYOUT_CONFIG.titleBoxTopPadding + textH + LAYOUT_CONFIG.titleBoxBottomPadding };
}

// Rendered once — title box + watermark, always visible for the full video.
async function createBaseOverlayImage(title) {
  const canvas = createCanvas(1080, 1920), ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, 1080, 1920);
  ctx.textBaseline = 'top'; ctx.textAlign = 'left';

  const { titleRes, boxH } = computeTitleBoxH(title);
  const textH = (titleRes.lines.length * titleRes.fontSize) + ((titleRes.lines.length - 1) * LAYOUT_CONFIG.titleLineSpacing);

  ctx.fillStyle = 'black'; ctx.fillRect(0, 0, 1080, boxH);
  let currY = (boxH - textH) / 2;
  for (const line of titleRes.lines) {
    const lw = measureMixedText(ctx, line, titleRes.fontSize);
    await drawMixedText(ctx, line, (1080 - lw) / 2, currY, titleRes.fontSize, 'white');
    currY += titleRes.fontSize + LAYOUT_CONFIG.titleLineSpacing;
  }

  const wmW = measureMixedText(ctx, LAYOUT_CONFIG.watermarkText, LAYOUT_CONFIG.watermarkFontSize);
  ctx.save();
  ctx.globalAlpha = LAYOUT_CONFIG.watermarkOpacity;
  await drawMixedText(ctx, LAYOUT_CONFIG.watermarkText, 1080 - wmW - LAYOUT_CONFIG.watermarkPadding, 1920 - LAYOUT_CONFIG.watermarkFontSize - LAYOUT_CONFIG.watermarkPadding, LAYOUT_CONFIG.watermarkFontSize, 'white', 'black', LAYOUT_CONFIG.textOutlineWidth);
  ctx.restore();

  return canvas;
}

// Rendered once per rank — draws only that single rank entry, positioned
// using the pre-calculated boxH so it lines up with the base overlay.
async function createRankOverlayImage(ranks, rankIndex, boxH) {
  const canvas = createCanvas(1080, 1920), ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, 1080, 1920);
  ctx.textBaseline = 'top'; ctx.textAlign = 'left';

  const y = LAYOUT_CONFIG.rankPaddingY + boxH + (rankIndex * LAYOUT_CONFIG.rankSpacing);
  const rRes = fitTextToBox(ranks[rankIndex], LAYOUT_CONFIG.rankBoxWidth, LAYOUT_CONFIG.rankMaxLines, LAYOUT_CONFIG.rankFontSize);

  ctx.font = `${LAYOUT_CONFIG.rankFontSize}px "CustomFont"`;
  ctx.strokeStyle = 'black'; ctx.lineWidth = LAYOUT_CONFIG.textOutlineWidth;
  ctx.strokeText(`${rankIndex + 1}.`, LAYOUT_CONFIG.rankNumX, y);
  ctx.fillStyle = LAYOUT_CONFIG.rankColors[rankIndex] || 'white';
  ctx.fillText(`${rankIndex + 1}.`, LAYOUT_CONFIG.rankNumX, y);

  await drawMixedText(ctx, rRes.lines[0], LAYOUT_CONFIG.rankTextX, y + ((LAYOUT_CONFIG.rankFontSize - rRes.fontSize) / 2), rRes.fontSize, 'white', 'black', LAYOUT_CONFIG.textOutlineWidth);

  return canvas;
}

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

async function generateThumbnail(videoPath, outputPath) {
  return new Promise((resolve, reject) => {
    const args = ['-i', videoPath, '-ss', '00:00:01', '-vframes', '1', '-q:v', '2', '-y', outputPath];
    const proc = spawn('ffmpeg', args);
    let stderr = '';
    proc.stderr.on('data', (data) => { stderr += data.toString(); });
    proc.on('error', reject);
    proc.on('close', code => code === 0 ? resolve() : reject(new Error(`Thumbnail failed: ${code} ${stderr}`)));
  });
}

// --- Shared: Composite overlay PNG onto a video clip ---
function applyOverlay(inputPath, overlayPath, outputPath) {
  return new Promise((resolve, reject) => {
    const filter = `[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[v];[1:v]scale=1080:1920[ov];[v][ov]overlay=0:0`;
    const proc = spawn('ffmpeg', [
      '-i', inputPath, '-i', overlayPath,
      '-filter_complex', filter,
      '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23', '-c:a', 'aac',
      '-movflags', '+faststart', '-y', outputPath
    ]);
    let stderr = '';
    proc.stderr.on('data', d => { stderr += d.toString(); });
    proc.on('error', reject);
    proc.on('close', code => code === 0 ? resolve() : reject(new Error(`Overlay failed (${code}): ${stderr.slice(-300)}`)));
  });
}

// --- Shared: Stitch a list of clips into one file ---
function stitchClips(listPath, outputPath) {
  return new Promise((resolve, reject) => {
    const proc = spawn('ffmpeg', ['-f', 'concat', '-safe', '0', '-i', listPath, '-c', 'copy', '-movflags', '+faststart', '-y', outputPath]);
    let stderr = '';
    proc.stderr.on('data', d => { stderr += d.toString(); });
    proc.on('error', reject);
    proc.on('close', code => code === 0 ? resolve() : reject(new Error(`Stitch failed (${code}): ${stderr.slice(-300)}`)));
  });
}

// --- Pre-edited only: Cut a segment from a source file ---
function cutSegment(sourcePath, startSec, endSec, outputPath) {
  return new Promise((resolve, reject) => {
    const proc = spawn('ffmpeg', [
      '-ss', String(startSec),
      '-i', sourcePath,
      '-t', String(endSec - startSec),
      '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23',
      '-c:a', 'aac',
      '-movflags', '+faststart',
      '-y', outputPath
    ]);
    let stderr = '';
    proc.stderr.on('data', d => { stderr += d.toString(); });
    proc.on('error', reject);
    proc.on('close', code => code === 0 ? resolve() : reject(new Error(`Cut failed (${code}): ${stderr.slice(-300)}`)));
  });
}

// --- Pipeline: Auto-stitch (N separate uploaded clips) ---
async function processAutoStitch(req, res, { postId, title, ranks, filePaths }) {
  const tempFiles = [];
  try {
    await updateStatusFile(postId, 'PROCESSING', { progress: 5 });
    const parsedRanks = typeof ranks === 'string' ? JSON.parse(ranks) : ranks;

    // Download all clips
    const local = await Promise.all(filePaths.map(async (fp, i) => {
      const p = `/tmp/in_${i}_${uuidv4()}${path.extname(fp) || '.mp4'}`;
      await cacheBucket.file(fp).download({ destination: p });
      tempFiles.push(p);
      return p;
    }));

    // Overlay each clip
    const processed = [];
    for (let i = 0; i < local.length; i++) {
      const prog = 10 + Math.floor((i / local.length) * 60);
      await updateStatusFile(postId, 'PROCESSING', { progress: prog });

      const ovPath = `/tmp/ov_${i}_${uuidv4()}.png`;
      const outPath = `/tmp/proc_${i}_${uuidv4()}.mp4`;
      tempFiles.push(ovPath, outPath);

      const canvas = await createTextOverlayImage(title, parsedRanks, i + 1);
      fs.writeFileSync(ovPath, canvas.toBuffer('image/png'));
      await applyOverlay(local[i], ovPath, outPath);
      processed.push(outPath);
    }

    // Stitch
    await updateStatusFile(postId, 'PROCESSING', { progress: 80 });
    const listPath = `/tmp/l_${uuidv4()}.txt`;
    const finalPath = `/tmp/f_${uuidv4()}.mp4`;
    tempFiles.push(listPath, finalPath);
    fs.writeFileSync(listPath, processed.map(p => `file '${p}'`).join('\n'));
    await stitchClips(listPath, finalPath);

    // Thumbnail & upload
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

// --- Pipeline: Pre-edited (single source file, timed text overlays) ---
// The source video plays untouched. One base overlay (title + watermark) is
// always visible. Each rank gets its own PNG, enabled from its timestamp to
// endTime — so ranks stack up as the video progresses. Single FFmpeg pass.
async function processPreEdited(req, res, { postId, title, ranks, filePath, timestamps, endTime }) {
  const tempFiles = [];
  try {
    await updateStatusFile(postId, 'PROCESSING', { progress: 5 });
    const parsedRanks = typeof ranks === 'string' ? JSON.parse(ranks) : ranks;
    const parsedEndTime = typeof endTime === 'string' ? parseFloat(endTime) : endTime;

    // Download source file
    await updateStatusFile(postId, 'PROCESSING', { progress: 10 });
    const sourcePath = `/tmp/source_${uuidv4()}${path.extname(filePath) || '.mp4'}`;
    await cacheBucket.file(filePath).download({ destination: sourcePath });
    tempFiles.push(sourcePath);

    // Build overlay states — now we only need the timestamps, no ranksToShow state
    await updateStatusFile(postId, 'PROCESSING', { progress: 20 });
    const sortedTimestamps = [...timestamps].sort((a, b) => a.time - b.time);

    // Pre-calculate boxH once so rank overlays align with the base
    const { boxH } = computeTitleBoxH(title);

    // Title fade-out: base overlay disappears 200ms before endTime so it fades
    // with any fade-to-black rather than cutting off abruptly.
    const titleEnd = Math.max(0, parsedEndTime - 0.2);

    // Generate base overlay (title + watermark) — enabled from t=0 to titleEnd
    const basePath = `/tmp/base_${uuidv4()}.png`;
    tempFiles.push(basePath);
    fs.writeFileSync(basePath, (await createBaseOverlayImage(title)).toBuffer('image/png'));

    // Rank reveal order: highest rank index appears first (most suspense).
    // sortedTimestamps[0] is the earliest mark → assign to the last rank (parsedRanks.length - 1),
    // sortedTimestamps[1] → second-to-last rank, etc.
    // Each rank stays visible from its assigned timestamp until endTime so they accumulate.
    const rankPaths = [];
    for (let i = 0; i < parsedRanks.length; i++) {
      await updateStatusFile(postId, 'PROCESSING', { progress: 25 + Math.floor((i / parsedRanks.length) * 35) });
      const rankPath = `/tmp/rank_${i}_${uuidv4()}.png`;
      tempFiles.push(rankPath);
      // Reverse: timestamp slot 0 → last rank, slot 1 → second-to-last, etc.
      const rankIndex = parsedRanks.length - 1 - i;
      fs.writeFileSync(rankPath, (await createRankOverlayImage(parsedRanks, rankIndex, boxH)).toBuffer('image/png'));
      rankPaths.push({ path: rankPath, rankIndex, timestampSlot: i });
    }

    // Build single FFmpeg pass:
    //   [0:v]  scale+crop                                                   → [base_v]
    //   [1:v]  base overlay (title+watermark), enable='between(t,0,titleEnd)' → [v_base]
    //   [2:v]  rank N-1 PNG, enable='between(t, ts[0].time, endTime)'      → [v0]
    //   [3:v]  rank N-2 PNG, enable='between(t, ts[1].time, endTime)'      → [v1]
    //   ...rank 0 (highest priority / last revealed) at final timestamp
    await updateStatusFile(postId, 'PROCESSING', { progress: 65 });

    const finalPath = `/tmp/f_${uuidv4()}.mp4`;
    tempFiles.push(finalPath);

    const inputArgs = ['-i', sourcePath, '-i', basePath];
    for (const { path } of rankPaths) inputArgs.push('-i', path);

    const filterParts = [
      `[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[base_v]`,
      `[1:v]scale=1080:1920[base_ov]`,
      `[base_v][base_ov]overlay=0:0:enable='between(t,0,${titleEnd})'[v_base]`,
    ];
    let prevLabel = 'v_base';
    for (let i = 0; i < rankPaths.length; i++) {
      const { timestampSlot } = rankPaths[i];
      const start = sortedTimestamps[timestampSlot]?.time ?? 0;
      const inputIdx = i + 2;
      filterParts.push(`[${inputIdx}:v]scale=1080:1920[r${i}]`);
      filterParts.push(`[${prevLabel}][r${i}]overlay=0:0:enable='between(t,${start},${parsedEndTime})'[v${i}]`);
      prevLabel = `v${i}`;
    }

    await new Promise((resolve, reject) => {
      const proc = spawn('ffmpeg', [
        ...inputArgs,
        '-filter_complex', filterParts.join(';'),
        '-map', `[${prevLabel}]`,
        '-map', '0:a?',
        '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23',
        '-c:a', 'aac',
        '-movflags', '+faststart',
        '-y', finalPath
      ]);
      let stderr = '';
      proc.stderr.on('data', d => { stderr += d.toString(); });
      proc.on('error', reject);
      proc.on('close', code => code === 0 ? resolve() : reject(new Error(`FFmpeg overlay failed (${code}): ${stderr.slice(-500)}`)));
    });

    // Thumbnail & upload
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

// --- Main HTTP Function ---
functions.http('processVideos', async (req, res) => {
  const { action, videoCount, sessionId, fileTypes, fileType, title, ranks, filePaths, filePath, timestamps, endTime, postId, videoMode } = req.body;

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
