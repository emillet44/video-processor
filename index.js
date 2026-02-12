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
  titleFontSize: 100, titleLineSpacing: 60, titleBoxWidth: 980,
  titleMaxLines: 2, titleBoxTopPadding: 30, titleBoxBottomPadding: 40,
  rankFontSize: 60, rankSpacing: 140, rankPaddingY: 80, rankNumX: 45,
  rankTextX: 125, rankBoxWidth: 830, rankMaxLines: 1,
  watermarkText: 'ranktop.net', watermarkFontSize: 48, watermarkPadding: 20,
  textOutlineWidth: 12
};

const emojiCache = new Map();
if (fs.existsSync(LAYOUT_CONFIG.fontPath)) {
  registerFont(LAYOUT_CONFIG.fontPath, { family: 'CustomFont' });
}

// --- Status Management (JSON File) ---
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

// --- Database Webhook (Only used for final states) ---
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
  await drawMixedText(ctx, LAYOUT_CONFIG.watermarkText, 1080 - wmW - LAYOUT_CONFIG.watermarkPadding, 1920 - LAYOUT_CONFIG.watermarkFontSize - LAYOUT_CONFIG.watermarkPadding, LAYOUT_CONFIG.watermarkFontSize, 'white', 'black', LAYOUT_CONFIG.textOutlineWidth);

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

// --- Main HTTP Function ---
functions.http('processVideos', async (req, res) => {
  const { action, videoCount, sessionId, fileTypes, title, ranks, filePaths, postId } = req.body;

  // 1. Check Status (Used by Client Polling)
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

  // 2. Generate Upload URLs
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

  // 3. Main Processing Job
  if (!filePaths || !postId) {
    return res.status(400).json({ error: "Missing filePaths or postId" });
  }

  const tempFiles = [];
  try {
    // A. Start - Create Status File
    await updateStatusFile(postId, 'PROCESSING', { progress: 5 });
    const parsedRanks = typeof ranks === 'string' ? JSON.parse(ranks) : ranks;
    
    // B. Download
    const local = await Promise.all(filePaths.map(async (fp, i) => {
      const p = `/tmp/in_${i}_${uuidv4()}${path.extname(fp) || '.mp4'}`;
      await cacheBucket.file(fp).download({ destination: p });
      tempFiles.push(p); return p;
    }));

    // C. Render Segments
    const processed = [];
    for (let i = 0; i < local.length; i++) {
      // Update progress occasionally
      const prog = 10 + Math.floor((i / local.length) * 60);
      await updateStatusFile(postId, 'PROCESSING', { progress: prog });

      const out = `/tmp/proc_${i}_${uuidv4()}.mp4`, ov = `/tmp/ov_${i}_${uuidv4()}.png`;
      tempFiles.push(out, ov);
      const canvas = await createTextOverlayImage(title, parsedRanks, i + 1);
      fs.writeFileSync(ov, canvas.toBuffer('image/png'));

      await new Promise((resolve, reject) => {
        const filter = `[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[v];[1:v]scale=1080:1920[ov];[v][ov]overlay=0:0`;
        const proc = spawn('ffmpeg', [
          '-i', local[i], '-i', ov, 
          '-filter_complex', filter, 
          '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23', '-c:a', 'aac', 
          '-movflags', '+faststart', '-y', out
        ]);
        proc.on('error', reject);
        proc.on('close', c => c === 0 ? resolve() : reject(new Error(`FFmpeg error code ${c}`)));
      });
      processed.push(out);
    }

    // D. Stitch
    await updateStatusFile(postId, 'PROCESSING', { progress: 80 });
    const final = `/tmp/f_${uuidv4()}.mp4`, list = `/tmp/l_${uuidv4()}.txt`;
    tempFiles.push(final, list);
    fs.writeFileSync(list, processed.map(p => `file '${p}'`).join('\n'));
    
    await new Promise((resolve, reject) => {
      const proc = spawn('ffmpeg', ['-f', 'concat', '-safe', '0', '-i', list, '-c', 'copy', '-movflags', '+faststart', '-y', final]);
      proc.on('error', reject);
      proc.on('close', c => c === 0 ? resolve() : reject(new Error('Stitch failed')));
    });

    // E. Thumbnail & Upload
    await updateStatusFile(postId, 'PROCESSING', { progress: 90 });
    const thumb = `/tmp/t_${uuidv4()}.jpg`;
    tempFiles.push(thumb);
    await generateThumbnail(final, thumb);

    await Promise.all([
      outputBucket.upload(final, { destination: `${postId}.mp4` }),
      thumbnailBucket.upload(thumb, { destination: `${postId}.jpg`, metadata: { contentType: 'image/jpeg' } })
    ]);

    // F. SUCCESS: Notify Status File AND Database
    await updateStatusFile(postId, 'READY', { progress: 100 });
    await notifyWebsite(postId, 'READY', null, req);
    
    res.status(200).json({ status: 'SUCCESS' });
    
  } catch (error) {
    console.error("Job Error:", error);
    // Failure: Notify both
    await updateStatusFile(postId, 'FAILED', { error: error.message });
    await notifyWebsite(postId, 'FAILED', error.message, req);
    res.status(500).json({ error: error.message });
  } finally {
    tempFiles.forEach(f => { try { fs.unlinkSync(f); } catch {} });
    emojiCache.clear();
  }
});
