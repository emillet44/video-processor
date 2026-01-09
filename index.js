const functions = require('@google-cloud/functions-framework');
const { Storage } = require('@google-cloud/storage');
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const { createCanvas, registerFont } = require('canvas');

const storage = new Storage();
const cacheBucket = storage.bucket('ranktop-v-cache');
const outputBucket = storage.bucket('ranktop-v');

const QUALITY_PRESETS = {
  preview: {
    targetW: 720,
    targetH: 1280,
    preset: 'ultrafast',
    crf: '28',
    path: 'previews'
  },
  final: {
    targetW: 1080,
    targetH: 1920,
    preset: 'ultrafast',
    crf: '23',
    path: 'posts'
  }
};

const LAYOUT_CONFIG = {
  titleFontSize: 100,
  titleY: 0,
  titleBoxTopPadding: 30,
  titleBoxBottomPadding: 40,
  titleLineSpacing: 60,
  titleBoxWidth: 980,
  titleMaxLines: 2,
  rankFontSize: 60,
  rankPaddingY: 80,
  rankSpacing: 140,
  rankNumX: 45,
  rankTextX: 125,
  rankBoxWidth: 830,
  rankMaxLines: 1,
  rankColors: ['#FFD700', '#C0C0C0', '#CD7F32', 'white', 'white'],
  watermarkText: 'ranktop.net',
  watermarkFontSize: 48,
  watermarkAlpha: '0.7',
  watermarkPadding: 20,
  fontPath: '/usr/share/fonts/truetype/font.ttf',
  textOutlineWidth: 12
};

const getRankColor = (idx) => LAYOUT_CONFIG.rankColors[idx] || 'white';

if (!fs.existsSync(LAYOUT_CONFIG.fontPath)) {
  throw new Error(`Font file not found at ${LAYOUT_CONFIG.fontPath}`);
}
registerFont(LAYOUT_CONFIG.fontPath, { family: 'CustomFont' });

function fitTextToBox(text, boxWidth, maxLines, initialFontSize) {
  const canvas = createCanvas(boxWidth, 100);
  const ctx = canvas.getContext('2d');
  for (let fontSize = initialFontSize; fontSize >= 1; fontSize -= 2) {
    ctx.font = `${fontSize}px CustomFont`;
    const words = text.split(' ');
    const lines = [];
    let currentLine = '';
    for (const word of words) {
      const testLine = currentLine ? `${currentLine} ${word}` : word;
      if (ctx.measureText(testLine).width <= boxWidth) {
        currentLine = testLine;
      } else {
        if (currentLine) { lines.push(currentLine); currentLine = word; }
        else { lines.push(word); currentLine = ''; }
      }
    }
    if (currentLine) lines.push(currentLine);
    if (lines.length <= maxLines) return { fontSize, lines };
  }
}

function createTextOverlayImage(title, ranks, ranksToShow, targetW, targetH) {
  const canvas = createCanvas(targetW, targetH);
  const ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, targetW, targetH);
  ctx.textBaseline = 'top';
  ctx.textAlign = 'left';

  // Scale factor if we are doing 720p preview
  const scale = targetW / 1080;

  const titleResult = fitTextToBox(title, LAYOUT_CONFIG.titleBoxWidth * scale, LAYOUT_CONFIG.titleMaxLines, LAYOUT_CONFIG.titleFontSize * scale);
  const numLines = titleResult.lines.length;
  const textContentHeight = (numLines * titleResult.fontSize) + ((numLines - 1) * LAYOUT_CONFIG.titleLineSpacing * scale);
  const boxHeight = (LAYOUT_CONFIG.titleBoxTopPadding + LAYOUT_CONFIG.titleBoxBottomPadding) * scale + textContentHeight;

  ctx.fillStyle = 'black';
  ctx.fillRect(0, 0, targetW, boxHeight);
  ctx.fillStyle = 'white';
  ctx.font = `${titleResult.fontSize}px CustomFont`;
  let currentY = (boxHeight - textContentHeight) / 2;
  for (const line of titleResult.lines) {
    const x = (targetW - ctx.measureText(line).width) / 2;
    ctx.fillText(line, x, currentY);
    currentY += titleResult.fontSize + (LAYOUT_CONFIG.titleLineSpacing * scale);
  }

  const startRankIdx = ranks.length - ranksToShow;
  for (let i = 0; i < ranksToShow; i++) {
    const rankIdx = startRankIdx + i;
    const y = (LAYOUT_CONFIG.rankPaddingY * scale) + boxHeight + (rankIdx * LAYOUT_CONFIG.rankSpacing * scale);
    const rankResult = fitTextToBox(ranks[rankIdx], LAYOUT_CONFIG.rankBoxWidth * scale, LAYOUT_CONFIG.rankMaxLines, LAYOUT_CONFIG.rankFontSize * scale);

    ctx.font = `${LAYOUT_CONFIG.rankFontSize * scale}px CustomFont`;
    const numText = `${rankIdx + 1}.`;
    ctx.strokeStyle = 'black';
    ctx.lineWidth = LAYOUT_CONFIG.textOutlineWidth * scale;
    ctx.strokeText(numText, LAYOUT_CONFIG.rankNumX * scale, y);
    ctx.fillStyle = getRankColor(rankIdx);
    ctx.fillText(numText, LAYOUT_CONFIG.rankNumX * scale, y);

    ctx.font = `${rankResult.fontSize}px CustomFont`;
    const rankTextY = y + ((LAYOUT_CONFIG.rankFontSize * scale - rankResult.fontSize) / 2);
    ctx.strokeText(rankResult.lines[0], LAYOUT_CONFIG.rankTextX * scale, rankTextY);
    ctx.fillStyle = 'white';
    ctx.fillText(rankResult.lines[0], LAYOUT_CONFIG.rankTextX * scale, rankTextY);
  }

  ctx.font = `${LAYOUT_CONFIG.watermarkFontSize * scale}px CustomFont`;
  const wmMetrics = ctx.measureText(LAYOUT_CONFIG.watermarkText);
  const wmX = targetW - wmMetrics.width - (LAYOUT_CONFIG.watermarkPadding * scale);
  const wmY = targetH - (LAYOUT_CONFIG.watermarkFontSize * scale) - (LAYOUT_CONFIG.watermarkPadding * scale);
  ctx.strokeStyle = 'black';
  ctx.lineWidth = LAYOUT_CONFIG.textOutlineWidth * scale;
  ctx.strokeText(LAYOUT_CONFIG.watermarkText, wmX, wmY);
  ctx.fillStyle = 'white';
  ctx.fillText(LAYOUT_CONFIG.watermarkText, wmX, wmY);

  return canvas;
}

functions.http('processVideos', async (req, res) => {
  res.set({ 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'Content-Type' });
  if (req.method === 'OPTIONS') return res.status(204).send('');
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const { action } = req.body;
  if (action === 'getUploadUrls') {
    try {
      const { videoCount, sessionId, fileTypes } = req.body;
      const uploadUrls = [];
      const filePaths = [];
      for (let i = 0; i < videoCount; i++) {
        const contentType = fileTypes?.[i] || 'video/mp4';
        const fileName = `${sessionId}/video_${i}.${contentType.split('/')[1] || 'mp4'}`;
        const [url] = await cacheBucket.file(fileName).getSignedUrl({ version: 'v4', action: 'write', expires: Date.now() + 15 * 60 * 1000, contentType });
        uploadUrls.push({ index: i, url });
        filePaths.push(fileName);
      }
      return res.json({ uploadUrls, filePaths, sessionId });
    } catch (error) { return res.status(500).json({ error: 'Upload URL failed' }); }
  }

  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive' });
  const tracker = new ProgressTracker(res);

  try {
    const { sessionId, title, ranks, filePaths, quality = 'preview', postId } = req.body;
    const preset = QUALITY_PRESETS[quality];
    if (!preset) throw new Error('Invalid quality');
    if (quality === 'final' && !postId) throw new Error('postId missing');

    const parsedRanks = typeof ranks === 'string' ? JSON.parse(ranks) : ranks;
    tracker.update('Downloading fragments...', 15);
    const localFiles = await downloadVideos(filePaths);

    tracker.update(`Parallel Processing (${quality})...`, 40);
    const processedVideos = await processVideosParallel(localFiles, title, parsedRanks, preset);

    tracker.update('Assembling final video...', 80);
    const finalPath = await concatenateVideos(processedVideos);

    const destName = quality === 'preview' ? `${sessionId}.mp4` : `${postId}.mp4`;
    const finalUrl = await uploadToGCS(finalPath, destName, preset.path);

    tracker.update('Finalizing...', 95);
    await cleanup(sessionId, [...localFiles, ...processedVideos, finalPath]);
    tracker.complete(finalUrl);
  } catch (error) {
    tracker.error(error.message);
    await cleanup(req.body.sessionId).catch(() => {});
  }
});

async function downloadVideos(filePaths) {
  return Promise.all(filePaths.map(async (fp, i) => {
    const local = `/tmp/in_${i}_${uuidv4()}${path.extname(fp) || '.mp4'}`;
    await cacheBucket.file(fp).download({ destination: local });
    return local;
  }));
}

async function processVideosParallel(files, title, ranks, preset) {
  return Promise.all(files.map(async (file, i) => {
    const out = `/tmp/proc_${i}_${uuidv4()}.mp4`;
    const canvas = createTextOverlayImage(title, ranks, i + 1, preset.targetW, preset.targetH);
    const ovPath = `/tmp/ov_${i}_${uuidv4()}.png`;
    fs.writeFileSync(ovPath, canvas.toBuffer('image/png'));

    await new Promise((resolve, reject) => {
      const filter = `[0:v]scale=${preset.targetW}:${preset.targetH}:force_original_aspect_ratio=increase,crop=${preset.targetW}:${preset.targetH}[v];[1:v]scale=${preset.targetW}:${preset.targetH}[ov];[v][ov]overlay=0:0`;
      const args = ['-i', file, '-i', ovPath, '-filter_complex', filter, '-c:v', 'libx264', '-preset', preset.preset, '-crf', preset.crf, '-c:a', 'aac', '-movflags', '+faststart', '-y', out];
      const proc = spawn('ffmpeg', args);
      proc.on('close', (code) => {
        fs.unlinkSync(ovPath);
        code === 0 ? resolve() : reject(new Error(`FFmpeg error ${code}`));
      });
    });
    return out;
  }));
}

function concatenateVideos(paths) {
  return new Promise((resolve, reject) => {
    const out = `/tmp/fin_${uuidv4()}.mp4`;
    const list = `/tmp/list_${uuidv4()}.txt`;
    fs.writeFileSync(list, paths.map(p => `file '${p}'`).join('\n'));
    const proc = spawn('ffmpeg', ['-f', 'concat', '-safe', '0', '-i', list, '-c', 'copy', '-movflags', '+faststart', '-y', out]);
    proc.on('close', (code) => {
      fs.unlinkSync(list);
      code === 0 ? resolve(out) : reject(new Error('Concat failed'));
    });
  });
}

async function uploadToGCS(filePath, fileName, subPath) {
  const dest = `${subPath}/${fileName}`;
  await outputBucket.upload(filePath, { destination: dest, metadata: { cacheControl: 'public, max-age=31536000' } });
  const [url] = await outputBucket.file(dest).getSignedUrl({ version: 'v4', action: 'read', expires: Date.now() + 3600000 });
  return url;
}

class ProgressTracker {
  constructor(res) { this.res = res; this.step = 0; this.total = 6; }
  update(msg, prog) {
    this.step++;
    this.res.write(`data: ${JSON.stringify({ step: this.step, totalSteps: this.total, progress: prog, message: msg, timestamp: Date.now() })}\n\n`);
  }
  complete(videoUrl) {
    this.res.write(`data: ${JSON.stringify({ step: this.total, progress: 100, message: 'Complete!', videoUrl, complete: true, timestamp: Date.now() })}\n\n`);
    this.res.end();
  }
  error(err) { this.res.write(`data: ${JSON.stringify({ error: err })}\n\n`); this.res.end(); }
}

async function cleanup(sid, files = []) {
  files.forEach(f => { try { if (fs.existsSync(f)) fs.unlinkSync(f); } catch {} });
  if (sid) {
    const [remote] = await cacheBucket.getFiles({ prefix: `${sid}/` });
    await Promise.all(remote.map(f => f.delete().catch(() => {})));
  }
}
