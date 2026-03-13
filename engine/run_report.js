/**
 * Report Engine Runner
 * Spawns the Python pipeline as a child process from Node.js.
 * Updates the database with progress and final report URL.
 */

const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

// Environment variables for API keys (set in Railway)
const ENV_VARS = {
    PERPLEXITY_API_KEY: process.env.PERPLEXITY_API_KEY || '',
    OPENAI_API_KEY: process.env.OPENAI_API_KEY || '',
    CASHMERE_API_KEY: process.env.CASHMERE_API_KEY || '',
    DATAFORSEO_LOGIN: process.env.DATAFORSEO_LOGIN || '',
    DATAFORSEO_PASSWORD: process.env.DATAFORSEO_PASSWORD || '',
    AHREFS_API_KEY: process.env.AHREFS_API_KEY || '',
};

/**
 * Run the report generation pipeline.
 * @param {Object} params - Report parameters
 * @param {string} params.reportId - Unique report ID
 * @param {string} params.brandName - Company/brand name
 * @param {string} params.domain - Company domain (e.g. "acme.com")
 * @param {string} params.market - Target market
 * @param {string} params.analysisLens - Analysis type
 * @param {Object} db - SQLite database instance
 * @returns {Promise<string>} Path to generated report
 */
function runReport({ reportId, brandName, domain, market, analysisLens }, db) {
    return new Promise((resolve, reject) => {
        const projectRoot = path.join(__dirname, '..');
        const volumeMount = process.env.RAILWAY_VOLUME_MOUNT_PATH || '';
        const reportsBase = volumeMount ? path.join(volumeMount, 'reports') : path.join(projectRoot, 'reports');
        const outputDir = path.join(reportsBase, reportId);

        fs.mkdirSync(reportsBase, { recursive: true });
        fs.mkdirSync(outputDir, { recursive: true });
        fs.mkdirSync(path.join(outputDir, 'assets'), { recursive: true });

        console.log(`[engine] Starting report generation for ${brandName} (${domain})`);
        console.log(`[engine] Output dir: ${outputDir}`);

        try {
            db.prepare(`UPDATE reports SET status = 'collecting' WHERE id = ?`).run(reportId);
        } catch (e) {
            console.error('[engine] DB update error:', e);
        }

        const pythonScript = path.join(projectRoot, 'engine', 'pipeline_v3.py');
        const python = process.env.PYTHON_PATH || 'python3';

        const args = [
            pythonScript,
            '--brand', brandName,
            '--domain', domain,
            '--market', market,
            '--lens', analysisLens || 'Commercial diligence',
            '--report-id', reportId,
            '--output-dir', outputDir,
        ];

        console.log(`[engine] Running: ${python} ${args.join(' ')}`);

        const child = spawn(python, args, {
            cwd: projectRoot,
            env: { ...process.env, ...ENV_VARS, PYTHONUNBUFFERED: '1' },
            stdio: ['pipe', 'pipe', 'pipe'],
        });

        let stdout = '';
        let stderr = '';

        child.stdout.on('data', (data) => {
            const text = data.toString();
            stdout += text;
            console.log(`[engine:stdout] ${text.trim()}`);

            if (text.includes('Phase 1')) {
                try { db.prepare(`UPDATE reports SET status = 'collecting' WHERE id = ?`).run(reportId); } catch (e) {}
            } else if (text.includes('Phase 2')) {
                try { db.prepare(`UPDATE reports SET status = 'analyzing' WHERE id = ?`).run(reportId); } catch (e) {}
            } else if (text.includes('Phase 3')) {
                try { db.prepare(`UPDATE reports SET status = 'assembling' WHERE id = ?`).run(reportId); } catch (e) {}
            }
        });

        child.stderr.on('data', (data) => {
            stderr += data.toString();
            console.error(`[engine:stderr] ${data.toString().trim()}`);
        });

        child.on('close', (code) => {
            console.log(`[engine] Pipeline exited with code ${code}`);

            if (code === 0) {
                const reportUrl = `/reports/${reportId}`;
                const reportHtmlPath = path.join(outputDir, 'index.html');

                if (fs.existsSync(reportHtmlPath)) {
                    try {
                        db.prepare(`
                            UPDATE reports 
                            SET status = 'completed', report_url = ?, completed_at = ?
                            WHERE id = ?
                        `).run(reportUrl, new Date().toISOString(), reportId);
                    } catch (e) {
                        console.error('[engine] DB update error:', e);
                    }

                    console.log(`[engine] Report complete: ${reportUrl}`);
                    resolve(reportUrl);
                } else {
                    const error = `Report HTML not found at ${reportHtmlPath}`;
                    try {
                        db.prepare(`UPDATE reports SET status = 'failed', notes = ? WHERE id = ?`).run(error, reportId);
                    } catch (e) {}
                    reject(new Error(error));
                }
            } else {
                const lines = stderr.split('\n');
                let errorSummary = '';
                for (let i = lines.length - 1; i >= 0; i--) {
                    if (lines[i].includes('Error') || lines[i].includes('Exception') || lines[i].includes('Traceback')) {
                        errorSummary = lines.slice(Math.max(0, i - 2)).join('\n').slice(0, 800);
                        break;
                    }
                }
                if (!errorSummary) errorSummary = stderr.slice(-500);
                const error = `Pipeline failed with code ${code}: ${errorSummary}`;
                try {
                    db.prepare(`UPDATE reports SET status = 'failed', notes = ? WHERE id = ?`).run(error.slice(0, 1000), reportId);
                } catch (e) {}
                reject(new Error(error));
            }
        });

        child.on('error', (err) => {
            console.error('[engine] Spawn error:', err);
            try {
                db.prepare(`UPDATE reports SET status = 'failed', notes = ? WHERE id = ?`).run(err.message, reportId);
            } catch (e) {}
            reject(err);
        });

        const timeout = setTimeout(() => {
            console.error('[engine] Pipeline timed out after 25 minutes');
            child.kill('SIGKILL');
            try {
                db.prepare(`UPDATE reports SET status = 'failed', notes = 'Generation timed out' WHERE id = ?`).run(reportId);
            } catch (e) {}
            reject(new Error('Report generation timed out'));
        }, 25 * 60 * 1000);

        child.on('close', () => clearTimeout(timeout));
    });
}

module.exports = { runReport };
