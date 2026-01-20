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

const LAYOUT_CONFIG = {
  titleFontSize: 100,
  titleLineSpacing: 60,
  titleBoxWidth: 980,
  titleMaxLines: 2,
  titleBoxTopPadding: 30,
  titleBoxBottomPadding: 40,
  rankFontSize: 60,
  rankSpacing: 140,
  rankPaddingY: 80,
  rankNumX: 45,
  rankTextX: 125,
  rankBoxWidth: 830,
  rankMaxLines: 1,
  rankColors: ['#FFD700', '#C0C0C0', '#CD7F32', 'white', 'white'],
  watermarkText: 'ranktop.net',
  watermarkFontSize: 48,
  watermarkPadding: 20,
  textOutlineWidth: 12,
  fontPath: '/usr/share/fonts/truetype/custom/font.ttf',
  chineseFont: 'Noto Sans CJK SC',
  emojiFont: 'Noto Color Emoji'
};

const emojiCache = new Map();

if (fs.existsSync(LAYOUT_CONFIG.fontPath)) {
  registerFont(LAYOUT_CONFIG.fontPath, { family: 'CustomFont' });
} else {
  throw new Error(`Main font missing at ${LAYOUT_CONFIG.fontPath}`);
}

// --- Text & Emoji Utilities ---

function getEmojiUrl(emoji) {
  const codePoints = Array.from(emoji)
    .map(c => c.codePointAt(0).toString(16))
    .join('-');
  return `https://cdn.jsdelivr.net/gh/jdecked/twemoji@15.0.3/assets/72x72/${codePoints}.png`;
}

function getFontForChar(char) {
  const isCJK = /[\u4e00-\u9fa5]|[\u3040-\u30ff]|[\uff00-\uffef]/.test(char);
  const isEmoji = /\p{Extended_Pictographic}/u.test(char);
  if (isEmoji) return 'Emoji';
  if (isCJK) return LAYOUT_CONFIG.chineseFont;
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
      for (const emoji of Array.from(s.text)) {
        try {
          const url = getEmojiUrl(emoji);
          let img = emojiCache.get(url) || await loadImage(url);
          emojiCache.set(url, img);
          ctx.drawImage(img, currentX, y + (fontSize * 0.1), fontSize, fontSize);
          currentX += fontSize;
        } catch (e) { currentX += fontSize; }
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

async function createTextOverlayImage(title, ranks, ranksToShow) {
  const width = 1080, height = 1920;
  const canvas = createCanvas(width, height), ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, width, height);
  ctx.textBaseline = 'top'; ctx.textAlign = 'left';

  const titleRes = fitTextToBox(title, LAYOUT_CONFIG.titleBoxWidth, LAYOUT_CONFIG.titleMaxLines, LAYOUT_CONFIG.titleFontSize);
  const textH = (titleRes.lines.length * titleRes.fontSize) + ((titleRes.lines.length - 1) * LAYOUT_CONFIG.titleLineSpacing);
  const boxH = LAYOUT_CONFIG.titleBoxTopPadding + textH + LAYOUT_CONFIG.titleBoxBottomPadding;

  ctx.fillStyle = 'black'; ctx.fillRect(0, 0, width, boxH);
  let currY = (boxH - textH) / 2;
  for (const line of titleRes.lines) {
    const lw = measureMixedText(ctx, line, titleRes.fontSize);
    await drawMixedText(ctx, line, (width - lw) / 2, currY, titleRes.fontSize, 'white');
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

    const rankTextY = y + ((LAYOUT_CONFIG.rankFontSize - rRes.fontSize) / 2);
    await drawMixedText(ctx, rRes.lines[0], LAYOUT_CONFIG.rankTextX, rankTextY, rRes.fontSize, 'white', 'black', LAYOUT_CONFIG.textOutlineWidth);
  }

  // Watermark
  const wmSize = LAYOUT_CONFIG.watermarkFontSize;
  ctx.font = `${wmSize}px "CustomFont"`;
  const wmW = measureMixedText(ctx, LAYOUT_CONFIG.watermarkText, wmSize);
  const wmX = width - wmW - LAYOUT_CONFIG.watermarkPadding;
  const wmY = height - wmSize - LAYOUT_CONFIG.watermarkPadding;
  await drawMixedText(ctx, LAYOUT_CONFIG.watermarkText, wmX, wmY, wmSize, 'white', 'black', LAYOUT_CONFIG.textOutlineWidth);

  return canvas;
}

// --- Main HTTP Function ---
functions.http('processVideos', async (req, res) => {
  res.set({ 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'Content-Type' });
  if (req.method === 'OPTIONS') return res.status(204).send('');

  const { action, videoCount, sessionId, fileTypes, title, ranks, filePaths, postId } = req.body;

  if (action === 'getUploadUrls') {
    const uploadUrls = [], filePathsResult = [];
    for (let i = 0; i < videoCount; i++) {
      const type = fileTypes?.[i] || 'video/mp4';
      const fileName = `${sessionId}/v_${i}.${type.split('/')[1] || 'mp4'}`;
      const [url] = await cacheBucket.file(fileName).getSignedUrl({ version: 'v4', action: 'write', expires: Date.now() + 900000, contentType: type });
      uploadUrls.push({ index: i, url }); filePathsResult.push(fileName);
    }
    return res.json({ uploadUrls, filePaths: filePathsResult, sessionId });
  }

  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive' });
  const tracker = new ProgressTracker(res);
  const tempFiles = [];

  try {
    const parsedRanks = typeof ranks === 'string' ? JSON.parse(ranks) : ranks;
    const totalSteps = filePaths.length;

    tracker.update('Downloading fragments...', 10);
    const local = await Promise.all(filePaths.map(async (fp, i) => {
      const p = `/tmp/in_${i}_${uuidv4()}${path.extname(fp) || '.mp4'}`;
      await cacheBucket.file(fp).download({ destination: p });
      tempFiles.push(p); return p;
    }));

    const processed = [];
    for (let i = 0; i < local.length; i++) {
      const out = `/tmp/proc_${i}_${uuidv4()}.mp4`, ov = `/tmp/ov_${i}_${uuidv4()}.png`;
      tempFiles.push(out, ov);

      tracker.update(`Rendering fragment ${i + 1} of ${totalSteps}...`, 15 + Math.floor((i / totalSteps) * 60));

      const canvas = await createTextOverlayImage(title, parsedRanks, i + 1);
      fs.writeFileSync(ov, canvas.toBuffer('image/png'));

      await new Promise((resolve, reject) => {
        const filter = `[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[v];[1:v]scale=1080:1920[ov];[v][ov]overlay=0:0`;
        const args = ['-i', local[i], '-i', ov, '-filter_complex', filter, '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23', '-c:a', 'aac', '-movflags', '+faststart', '-y', out];
        spawn('ffmpeg', args).on('error', reject).on('close', code => {
          try { if (fs.existsSync(ov)) fs.unlinkSync(ov); } catch {}
          code === 0 ? resolve() : reject(new Error(`FFmpeg error ${code}`));
        });
      });
      processed.push(out);
      tracker.update(`Fragment ${i + 1} processed`, 15 + Math.floor(((i + 1) / totalSteps) * 60));
    }

    tracker.update('Stitching final video...', 80);
    const final = `/tmp/f_${uuidv4()}.mp4`, list = `/tmp/l_${uuidv4()}.txt`;
    tempFiles.push(final, list);
    fs.writeFileSync(list, processed.map(p => `file '${p}'`).join('\n'));

    await new Promise((resolve, reject) => {
      spawn('ffmpeg', ['-f', 'concat', '-safe', '0', '-i', list, '-c', 'copy', '-movflags', '+faststart', '-y', final])
        .on('error', reject).on('close', c => c === 0 ? resolve() : reject(new Error('Stitch failed')));
    });

    tracker.update('Uploading to storage...', 90);
    const dest = `${postId}.mp4`;
    await outputBucket.upload(final, { destination: dest, metadata: { cacheControl: 'public, max-age=31536000' } });

    tracker.update('Cleaning cache...', 95);
    const [remoteFiles] = await cacheBucket.getFiles({ prefix: `${sessionId}/` });
    await Promise.all(remoteFiles.map(f => f.delete().catch(() => {})));

    tracker.complete(dest);
  } catch (error) {
    console.error('Final processing error:', error);
    tracker.error(error.message);
  } finally {
    tempFiles.forEach(f => { try { if (fs.existsSync(f)) fs.unlinkSync(f); } catch {} });
    emojiCache.clear();
  }
});

class ProgressTracker {
  constructor(res) { this.res = res; }
  update(msg, prog) { this.res.write(`data: ${JSON.stringify({ message: msg, progress: prog, timestamp: Date.now() })}\n\n`); }
  complete(dest) { this.res.write(`data: ${JSON.stringify({ complete: true, videoUrl: dest, message: 'Final Video Ready', progress: 100 })}\n\n`); this.res.end(); }
  error(err) { this.res.write(`data: ${JSON.stringify({ error: err })}\n\n`); this.res.end(); }
}
