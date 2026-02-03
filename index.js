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

async function notifyWebsite(postId, status, errorMessage = null, req = null) {
  // 1. Default to production
  let baseUrl = "https://ranktop.net";

  // 2. CHECK: Did requester say where to send the webhook?
  // look for a custom header 'x-callback-url'
  if (req && req.headers['x-callback-url']) {
    baseUrl = req.headers['x-callback-url'].replace(/\/$/, "");
    console.info(`Using override callback URL: ${baseUrl}`);
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
        const url = getEmojiUrl(emoji);
        try {
          let img = emojiCache.get(url);
          if (!img) {
            img = await loadImage(url);
            emojiCache.set(url, img);
          }
          ctx.drawImage(img, currentX, y + (fontSize * 0.1), fontSize, fontSize);
        } catch (e) {
          console.warn(`Emoji Load Failed: ${emoji}`);
        }
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

// --- Thumbnail Generation ---

async function generateThumbnail(videoPath, outputPath) {
  return new Promise((resolve, reject) => {
    // Extract frame at 1 second, high quality JPEG
    const args = [
      '-i', videoPath,
      '-ss', '00:00:01',
      '-vframes', '1',
      '-q:v', '2',
      '-y',
      outputPath
    ];

    spawn('ffmpeg', args)
      .on('error', reject)
      .on('close', code => {
        code === 0 ? resolve() : reject(new Error(`Thumbnail generation failed: ${code}`));
      });
  });
}

// --- Main HTTP Function ---

functions.http('processVideos', async (req, res) => {
  const { action, videoCount, sessionId, fileTypes, title, ranks, filePaths, postId } = req.body;

  // Signed URL Generation (Remains synchronous for the client)
  if (action === 'getUploadUrls') {
    const uploadUrls = [];
    for (let i = 0; i < videoCount; i++) {
      const contentType = fileTypes?.[i] || 'video/mp4';
      const fileName = `${sessionId}/v_${i}.${contentType.split('/')[1] || 'mp4'}`;
      const [url] = await cacheBucket.file(fileName).getSignedUrl({ 
        version: 'v4', action: 'write', expires: Date.now() + 900000, contentType 
      });
      uploadUrls.push({ index: i, url });
    }
    return res.json({ uploadUrls, sessionId });
  }

  // --- RENDERING PHASE ---
  // send the response IMMEDIATELY so Vercel doesn't time out.
  // The actual work happens after res.send() in the background of the Cloud Run instance.
  res.status(202).send({ status: 'Processing started' });

  const tempFiles = [];
  try {
    await notifyWebsite(postId, 'PROCESSING');
    const parsedRanks = typeof ranks === 'string' ? JSON.parse(ranks) : ranks;
    
    // Download
    const local = await Promise.all(filePaths.map(async (fp, i) => {
      const p = `/tmp/in_${i}_${uuidv4()}${path.extname(fp) || '.mp4'}`;
      await cacheBucket.file(fp).download({ destination: p });
      tempFiles.push(p); return p;
    }));

    // Process
    const processed = [];
    for (let i = 0; i < local.length; i++) {
      const out = `/tmp/proc_${i}_${uuidv4()}.mp4`, ov = `/tmp/ov_${i}_${uuidv4()}.png`;
      tempFiles.push(out, ov);
      const canvas = await createTextOverlayImage(title, parsedRanks, i + 1);
      fs.writeFileSync(ov, canvas.toBuffer('image/png'));

      await new Promise((resolve, reject) => {
        const filter = `[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[v];[1:v]scale=1080:1920[ov];[v][ov]overlay=0:0`;
        spawn('ffmpeg', ['-i', local[i], '-i', ov, '-filter_complex', filter, '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23', '-c:a', 'aac', '-movflags', '+faststart', '-y', out])
          .on('close', c => c === 0 ? resolve() : reject(new Error(`FFmpeg error`)));
      });
      processed.push(out);
    }

    // Stitch
    const final = `/tmp/f_${uuidv4()}.mp4`, list = `/tmp/l_${uuidv4()}.txt`;
    tempFiles.push(final, list);
    fs.writeFileSync(list, processed.map(p => `file '${p}'`).join('\n'));
    await new Promise((resolve, reject) => {
      spawn('ffmpeg', ['-f', 'concat', '-safe', '0', '-i', list, '-c', 'copy', '-movflags', '+faststart', '-y', final])
        .on('close', c => c === 0 ? resolve() : reject(new Error('Stitch failed')));
    });

    // Thumbnail & Upload
    const thumb = `/tmp/t_${uuidv4()}.jpg`;
    tempFiles.push(thumb);
    await generateThumbnail(final, thumb);

    await Promise.all([
      outputBucket.upload(final, { destination: `${postId}.mp4` }),
      thumbnailBucket.upload(thumb, { destination: `${postId}.jpg`, metadata: { contentType: 'image/jpeg' } })
    ]);

    await notifyWebsite(postId, 'READY');
  } catch (error) {
    console.error("Job Error:", error);
    await notifyWebsite(postId, 'FAILED', error.message);
  } finally {
    tempFiles.forEach(f => { try { fs.unlinkSync(f); } catch {} });
    emojiCache.clear();
  }
});
