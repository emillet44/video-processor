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
  watermarkPadding: 20,
  fontPath: '/usr/share/fonts/truetype/font.ttf',
  textOutlineWidth: 12
};

// Initialize Font
if (!fs.existsSync(LAYOUT_CONFIG.fontPath)) {
  throw new Error(`Font file not found at ${LAYOUT_CONFIG.fontPath}`);
}
registerFont(LAYOUT_CONFIG.fontPath, { family: 'CustomFont' });

/**
 * Text Wrapping Utility
 */
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
  return { fontSize: 10, lines: [text] };
}

/**
 * Image Overlay Generator (Fixed at 1080x1920)
 */
function createTextOverlayImage(title, ranks, ranksToShow) {
  const width = 1080;
  const height = 1920;
  const canvas = createCanvas(width, height);
  const ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, width, height);
  ctx.textBaseline = 'top';
  ctx.textAlign = 'left';

  // Title Logic
  const titleResult = fitTextToBox(title, LAYOUT_CONFIG.titleBoxWidth, LAYOUT_CONFIG.titleMaxLines, LAYOUT_CONFIG.titleFontSize);
  const numLines = titleResult.lines.length;
  const textContentHeight = (numLines * titleResult.fontSize) + ((numLines - 1) * LAYOUT_CONFIG.titleLineSpacing);
  const boxHeight = LAYOUT_CONFIG.titleBoxTopPadding + textContentHeight + LAYOUT_CONFIG.titleBoxBottomPadding;

  ctx.fillStyle = 'black';
  ctx.fillRect(0, 0, width, boxHeight);
  ctx.fillStyle = 'white';
  ctx.font = `${titleResult.fontSize}px CustomFont`;
  let currentY = (boxHeight - textContentHeight) / 2;
  for (const line of titleResult.lines) {
    const x = (width - ctx.measureText(line).width) / 2;
    ctx.fillText(line, x, currentY);
    currentY += titleResult.fontSize + LAYOUT_CONFIG.titleLineSpacing;
  }

  // Rank Logic
  const startIdx = ranks.length - ranksToShow;
  for (let i = 0; i < ranksToShow; i++) {
    const rankIdx = startIdx + i;
    const y = LAYOUT_CONFIG.rankPaddingY + boxHeight + (rankIdx * LAYOUT_CONFIG.rankSpacing);
    const rankResult = fitTextToBox(ranks[rankIdx], LAYOUT_CONFIG.rankBoxWidth, LAYOUT_CONFIG.rankMaxLines, LAYOUT_CONFIG.rankFontSize);

    ctx.font = `${LAYOUT_CONFIG.rankFontSize}px CustomFont`;
    const numText = `${rankIdx + 1}.`;
    ctx.strokeStyle = 'black';
    ctx.lineWidth = LAYOUT_CONFIG.textOutlineWidth;
    ctx.strokeText(numText, LAYOUT_CONFIG.rankNumX, y);
    ctx.fillStyle = LAYOUT_CONFIG.rankColors[rankIdx] || 'white';
    ctx.fillText(numText, LAYOUT_CONFIG.rankNumX, y);

    ctx.font = `${rankResult.fontSize}px CustomFont`;
    const rankTextY = y + ((LAYOUT_CONFIG.rankFontSize - rankResult.fontSize) / 2);
    ctx.strokeText(rankResult.lines[0], LAYOUT_CONFIG.rankTextX, rankTextY);
    ctx.fillStyle = 'white';
    ctx.fillText(rankResult.lines[0], LAYOUT_CONFIG.rankTextX, rankTextY);
  }

  // Watermark
  ctx.font = `${LAYOUT_CONFIG.watermarkFontSize}px CustomFont`;
  const wmMetrics = ctx.measureText(LAYOUT_CONFIG.watermarkText);
  const wmX = width - wmMetrics.width - LAYOUT_CONFIG.watermarkPadding;
  const wmY = height - LAYOUT_CONFIG.watermarkFontSize - LAYOUT_CONFIG.watermarkPadding;
  ctx.strokeStyle = 'black';
  ctx.lineWidth = LAYOUT_CONFIG.textOutlineWidth;
  ctx.strokeText(LAYOUT_CONFIG.watermarkText, wmX, wmY);
  ctx.fillStyle = 'white';
  ctx.fillText(LAYOUT_CONFIG.watermarkText, wmX, wmY);

  return canvas;
}

/**
 * Progress Reporting Class
 */
class ProgressTracker {
  constructor(res) { this.res = res; }
  update(msg, prog) {
    this.res.write(`data: ${JSON.stringify({ message: msg, progress: prog, timestamp: Date.now() })}\n\n`);
  }
  complete(url) {
    this.res.write(`data: ${JSON.stringify({ complete: true, videoUrl: url, message: 'Final Video Ready' })}\n\n`);
    this.res.end();
  }
  error(err) {
    this.res.write(`data: ${JSON.stringify({ error: err })}\n\n`);
    this.res.end();
  }
}

/**
 * MAIN HTTP HANDLER
 */
functions.http('processVideos', async (req, res) => {
  res.set({
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST',
    'Access-Control-Allow-Headers': 'Content-Type'
  });

  if (req.method === 'OPTIONS') return res.status(204).send('');
  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive' });

  const { sessionId, title, ranks, filePaths, postId } = req.body;
  const tracker = new ProgressTracker(res);

  if (!sessionId || !postId) {
    return tracker.error("Missing sessionId or postId for final processing");
  }

  try {
    const parsedRanks = typeof ranks === 'string' ? JSON.parse(ranks) : ranks;

    // 1. Download fragments from GCS Cache
    tracker.update('Downloading fragments...', 15);
    const localFiles = await Promise.all(filePaths.map(async (fp, i) => {
      const p = `/tmp/in_${i}_${uuidv4()}${path.extname(fp) || '.mp4'}`;
      await cacheBucket.file(fp).download({ destination: p });
      return p;
    }));

    // 2. Parallel Processing (1080p)
    tracker.update('Rendering 1080p overlays...', 40);
    const processedVideos = await Promise.all(localFiles.map(async (file, i) => {
      const out = `/tmp/proc_${i}_${uuidv4()}.mp4`;
      const ovPath = `/tmp/ov_${i}_${uuidv4()}.png`;
      const canvas = createTextOverlayImage(title, parsedRanks, i + 1);
      fs.writeFileSync(ovPath, canvas.toBuffer('image/png'));

      await new Promise((resolve, reject) => {
        const filter = `[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[v];[1:v]scale=1080:1920[ov];[v][ov]overlay=0:0`;
        const args = ['-i', file, '-i', ovPath, '-filter_complex', filter, '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23', '-c:a', 'aac', '-movflags', '+faststart', '-y', out];
        spawn('ffmpeg', args).on('close', code => {
          fs.unlinkSync(ovPath);
          code === 0 ? resolve() : reject(new Error(`FFmpeg error ${code}`));
        });
      });
      return out;
    }));

    // 3. Concatenate fragments
    tracker.update('Stitching final video...', 80);
    const finalPath = `/tmp/final_${uuidv4()}.mp4`;
    const listFile = `/tmp/list_${uuidv4()}.txt`;
    fs.writeFileSync(listFile, processedVideos.map(p => `file '${p}'`).join('\n'));

    await new Promise((resolve, reject) => {
      const args = ['-f', 'concat', '-safe', '0', '-i', listFile, '-c', 'copy', '-movflags', '+faststart', '-y', finalPath];
      spawn('ffmpeg', args).on('close', code => {
        fs.unlinkSync(listFile);
        code === 0 ? resolve() : reject(new Error('Concat failed'));
      });
    });

    // 4. Upload to ranktop-v bucket in the posts/ folder
    tracker.update('Uploading to storage...', 90);
    const destination = `posts/${postId}.mp4`;
    await outputBucket.upload(finalPath, {
      destination,
      metadata: { cacheControl: 'public, max-age=31536000' }
    });

    // 5. Cleanup local and GCS Cache fragments
    tracker.update('Cleaning cache...', 95);
    [...localFiles, ...processedVideos, finalPath].forEach(f => {
      try { if (fs.existsSync(f)) fs.unlinkSync(f); } catch {}
    });

    const [remoteFiles] = await cacheBucket.getFiles({ prefix: `${sessionId}/` });
    await Promise.all(remoteFiles.map(f => f.delete().catch(() => {})));

    tracker.complete(destination);

  } catch (error) {
    console.error('Final processing error:', error);
    tracker.error(error.message);
  }
});
