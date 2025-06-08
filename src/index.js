import { parse } from 'node-html-parser';

/**
 * FAQ Schema Extraction Proxy Worker - HTML Parser Version
 * Uses node-html-parser for robust HTML parsing instead of regex
 */
addEventListener('fetch', e => e.respondWith(handleRequest(e.request)));

async function handleRequest(request) {
  // Extract origin/referer early for logging
  const origin = request.headers.get('Origin');
  const referer = request.headers.get('Referer');
  
  const cors = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, HEAD, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };
  
  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: cors });
  }

  // Security: Origin/Referer checking
  const allowedOrigins = [
    'https://365i.co.uk',
    'https://www.365i.co.uk',
    'https://staging.365i.co.uk',
    'http://localhost:3000', // For local development
    'http://localhost:8080'  // For local development
  ];
  
  // Check if request comes from allowed origins (if origin header exists)
  if (origin && !allowedOrigins.some(allowed => origin.startsWith(allowed))) {
    console.log(`Blocked request from unauthorized origin: ${origin}`);
    return new Response(JSON.stringify({ 
      error: 'Unauthorized origin', 
      success: false,
      metadata: {
        warning: "This service is for FAQ extraction only. Abuse will result in blocking.",
        terms: "By using this service, you agree not to violate any website's terms of service."
      }
    }), {
      status: 403,
      headers: { 'Content-Type': 'application/json', ...cors },
    });
  }

  const url = new URL(request.url).searchParams.get('url');
  if (!url) {
    return new Response(JSON.stringify({ 
      error: 'URL parameter required', 
      success: false 
    }), {
      status: 400,
      headers: { 'Content-Type': 'application/json', ...cors },
    });
  }

  try {
    const targetUrl = new URL(url);
    
    // Security: Block internal/private IPs and localhost
    const hostname = targetUrl.hostname;
    
    const blockedPatterns = [
      /^localhost$/i,
      /^127\./,
      /^10\./,
      /^172\.(1[6-9]|2[0-9]|3[0-1])\./,
      /^192\.168\./,
      /\.local$/i,
      /^0\.0\.0\.0$/
    ];
    
    if (blockedPatterns.some(pattern => pattern.test(hostname))) {
      console.log(`Blocked request to internal/private URL: ${hostname}`);
      return new Response(JSON.stringify({ 
        error: 'Internal/private URLs not allowed', 
        success: false,
        metadata: {
          warning: "This service cannot access internal or private network addresses.",
          terms: "By using this service, you agree not to violate any website's terms of service."
        }
      }), {
        status: 403,
        headers: { 'Content-Type': 'application/json', ...cors },
      });
    }
    
    // Log the extraction request
    console.log(`FAQ extraction requested: ${url} from ${requestOrigin} at ${new Date().toISOString()}`);
    
    // Keep origin/referer for logging
    const requestOrigin = origin || referer || 'unknown origin';
    
    // Add cache buster
    targetUrl.searchParams.append('_cb', Date.now());
    
    // Fetch with timeout
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 10000); // 10 second timeout
    
    const resp = await fetch(targetUrl.toString(), {
      signal: controller.signal,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Cache-Control': 'no-cache',
      },
      cf: { cacheTtl: 0, cacheEverything: false },
    }).catch(err => {
      if (err.name === 'AbortError') {
        console.error(`Request timeout for ${url} after 10 seconds`);
        throw new Error('Request timeout - target site took too long to respond');
      }
      throw err;
    });
    
    clearTimeout(timeoutId);
    
    if (!resp.ok) {
      return new Response(JSON.stringify({ 
        error: `Fetch failed: ${resp.status}`, 
        success: false 
      }), {
        status: resp.status,
        headers: { 'Content-Type': 'application/json', ...cors },
      });
    }
    
    const ct = resp.headers.get('Content-Type') || '';
    if (!ct.includes('text/html')) {
      return new Response(JSON.stringify({ 
        error: 'Not HTML', 
        success: false 
      }), {
        status: 415,
        headers: { 'Content-Type': 'application/json', ...cors },
      });
    }
    
    const html = await resp.text();
    
    // Check HTML size limit (5MB)
    if (html.length > 5 * 1024 * 1024) {
      return new Response(JSON.stringify({ 
        error: 'HTML too large (>5MB)', 
        success: false 
      }), {
        status: 413,
        headers: { 'Content-Type': 'application/json', ...cors },
      });
    }
    
    // Parse HTML
    const root = parse(html, {
      lowerCaseTagName: false,
      comment: false,
      blockTextElements: {
        script: false,
        noscript: false,
        style: false,
      }
    });
    
    const title = root.querySelector('title')?.text || '';
    let allFaqs = [];
    
    // 1) Try JSON-LD
    try {
      const jsonLdFaqs = extractJsonLd(root);
      allFaqs = allFaqs.concat(jsonLdFaqs);
    } catch (e) {
      console.error('JSON-LD extraction failed:', e);
    }
    
    // 2) Try Microdata
    try {
      const microdataFaqs = extractMicrodata(root);
      allFaqs = allFaqs.concat(microdataFaqs);
    } catch (e) {
      console.error('Microdata extraction failed:', e);
    }
    
    // 3) Try RDFa
    try {
      const rdfaFaqs = extractRdfa(root);
      allFaqs = allFaqs.concat(rdfaFaqs);
    } catch (e) {
      console.error('RDFa extraction failed:', e);
    }
    
    // Deduplicate
    allFaqs = dedupe(allFaqs);
    
    if (allFaqs.length > 0) {
      console.log(`Successfully extracted ${allFaqs.length} FAQs from ${url}`);
      return new Response(JSON.stringify({
        success: true,
        source: url,
        faqs: allFaqs,
        metadata: { 
          extractionMethod: 'html-parser', 
          totalExtracted: allFaqs.length, 
          title: title,
          terms: "By using this service, you agree not to violate any website's terms of service."
        }
      }), { 
        headers: { 'Content-Type': 'application/json', ...cors } 
      });
    }
    
    // Check if markup exists
    const hasFaqMarkup = html.includes('schema.org/FAQPage') || 
                         html.includes('typeof="FAQPage"') ||
                         html.includes('"@type":"FAQPage"');
    
    if (hasFaqMarkup) {
      console.warn(`FAQ markup detected but extraction failed for ${url}`);
      return new Response(JSON.stringify({
        success: false,
        source: url,
        error: "Page contains FAQ markup but extraction failed. The structure might be non-standard.",
        metadata: {
          title: title,
          extractionMethod: "failed",
          terms: "By using this service, you agree not to violate any website's terms of service."
        }
      }), {
        headers: { 'Content-Type': 'application/json', ...cors }
      });
    }
    
    // No FAQs found
    console.log(`No FAQ markup found on ${url}`);
    return new Response(JSON.stringify({
      success: false,
      source: url,
      faqs: [],
      metadata: { 
        extractionMethod: 'none', 
        title: title,
        message: "No FAQ schema markup found on this page",
        terms: "By using this service, you agree not to violate any website's terms of service."
      }
    }), { 
      headers: { 'Content-Type': 'application/json', ...cors } 
    });
    
  } catch (err) {
    console.error(`Worker error for URL ${url} from ${origin || referer || 'unknown origin'}: ${err.message}`, err.stack);
    return new Response(JSON.stringify({ 
      error: err.message || 'Internal error', 
      success: false,
      metadata: {
        warning: "This service is for FAQ extraction only. Abuse will result in blocking.",
        terms: "By using this service, you agree not to violate any website's terms of service."
      }
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...cors },
    });
  }
}

function extractJsonLd(root) {
  const faqs = [];
  const scripts = root.querySelectorAll('script[type="application/ld+json"]');
  
  scripts.forEach(script => {
    try {
      const data = JSON.parse(script.innerHTML);
      const arr = Array.isArray(data) ? data : [data];
      arr.forEach(d => traverseLd(d, faqs));
    } catch (e) {
      // Skip invalid JSON
    }
  });
  
  return faqs;
}

function traverseLd(obj, out) {
  if (!obj || typeof obj !== 'object') return;
  
  const type = obj['@type'];
  
  if ((Array.isArray(type) ? type.includes('FAQPage') : type === 'FAQPage')) {
    let mainEntity = obj.mainEntity || obj['mainEntity'];
    if (mainEntity) {
      mainEntity = Array.isArray(mainEntity) ? mainEntity : [mainEntity];
      mainEntity.forEach(q => {
        if (q['@type'] === 'Question' && q.name) {
          let answer = '';
          if (q.acceptedAnswer) {
            answer = typeof q.acceptedAnswer === 'string'
              ? q.acceptedAnswer
              : (q.acceptedAnswer.text || '');
          }
          if (answer) {
            let id = q['@id'] || q.id || null;
            if (id && id.includes('#')) {
              id = id.split('#').pop();
            }
            out.push({ 
              question: q.name.trim(), 
              answer: answer.trim(), 
              id: id 
            });
          }
        }
      });
    }
  }
  
  if (Array.isArray(obj['@graph'])) {
    obj['@graph'].forEach(x => traverseLd(x, out));
  }
}

function extractMicrodata(root) {
  const faqs = [];
  
  // Find all Question itemscopes
  const questions = root.querySelectorAll('[itemscope][itemtype*="Question"]');
  
  questions.forEach(questionEl => {
    // Get ID
    const id = questionEl.getAttribute('id') || questionEl.getAttribute('itemid')?.split('#').pop() || null;
    
    // Get question text
    const nameEl = questionEl.querySelector('[itemprop="name"]');
    if (!nameEl) return;
    const question = nameEl.text.trim();
    
    // Get answer - try multiple approaches
    let answer = '';
    
    // Approach 1: Direct itemprop="text"
    const directTextEl = questionEl.querySelector('[itemprop="text"]');
    if (directTextEl) {
      answer = directTextEl.innerHTML.trim();
    } else {
      // Approach 2: Inside acceptedAnswer
      const acceptedAnswerEl = questionEl.querySelector('[itemprop="acceptedAnswer"]');
      if (acceptedAnswerEl) {
        const textEl = acceptedAnswerEl.querySelector('[itemprop="text"]');
        if (textEl) {
          answer = textEl.innerHTML.trim();
        }
      }
    }
    
    if (question && answer) {
      faqs.push({
        question: question,
        answer: answer,
        id: id
      });
    }
  });
  
  return faqs;
}

function extractRdfa(root) {
  const faqs = [];
  
  // Find all Question types
  const questions = root.querySelectorAll('[typeof="Question"]');
  
  questions.forEach(questionEl => {
    // Get ID
    const id = questionEl.getAttribute('id') || 
               questionEl.getAttribute('resource')?.split('#').pop() || null;
    
    // Get question text
    const nameEl = questionEl.querySelector('[property="name"]');
    if (!nameEl) return;
    const question = nameEl.text.trim();
    
    // Get answer
    const textEl = questionEl.querySelector('[property="text"]');
    if (!textEl) return;
    const answer = textEl.innerHTML.trim();
    
    if (question && answer) {
      faqs.push({
        question: question,
        answer: answer,
        id: id
      });
    }
  });
  
  return faqs;
}

function dedupe(arr) {
  const seen = new Set();
  return arr.filter(faq => {
    if (!faq.question || !faq.answer) return false;
    if (faq.question.includes('${') || faq.answer.includes('${')) return false;
    
    const key = faq.question.toLowerCase().trim();
    if (seen.has(key)) return false;
    
    seen.add(key);
    return true;
  });
}
