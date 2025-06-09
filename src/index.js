import { parse } from 'node-html-parser';

/**
 * Enhanced FAQ Schema Extraction Proxy Worker
 * - Handles nested schemas, comments, multiple formats
 * - Processes images with verification
 * - Robust HTML sanitization
 * - Comprehensive metadata and warnings
 */
addEventListener('fetch', e => e.respondWith(handleRequest(e.request, e)));

async function handleRequest(request, event) {
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

  // Security: Origin/Referer checking - FIXED VERSION
  const allowedOrigins = [
    'https://365i.co.uk',
    'https://www.365i.co.uk',
    'https://staging.365i.co.uk',
    'http://localhost:3000',
    'http://localhost:8080'
  ];
  
  // More flexible origin checking
  if (origin || referer) {
    const checkOrigin = origin || referer;
    const isAllowed = allowedOrigins.some(allowed => {
      // Check if the origin/referer starts with allowed origin
      return checkOrigin.startsWith(allowed) ||
        // Also check without www
        checkOrigin.startsWith(allowed.replace('www.', '')) ||
        // And with www if not present
        checkOrigin.startsWith(allowed.replace('://', '://www.'));
    });
    
    if (!isAllowed) {
      console.log(`Request from origin: ${origin}, referer: ${referer}`);
      console.log(`Blocked request from unauthorized origin: ${checkOrigin}`);
      return new Response(JSON.stringify({
        error: 'Unauthorized origin',
        success: false,
        debug: {
          origin: origin,
          referer: referer,
          checkedAgainst: checkOrigin
        },
        metadata: {
          warning: "This service is for FAQ extraction only. Abuse will result in blocking.",
          terms: "By using this service, you agree not to violate any website's terms of service."
        }
      }), {
        status: 403,
        headers: { 'Content-Type': 'application/json', ...cors },
      });
    }
  }

  // RATE LIMITING - Check before processing request
  const DAILY_LIMIT = 100; // Adjust this as needed
  const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
  const today = new Date().toISOString().split('T')[0];
  const rateLimitKey = `faq-proxy:${clientIP}:${today}`;
  
  try {
    // Get current usage from KV
    let usageData = await event.env.FAQ_RATE_LIMITS.get(rateLimitKey, { type: 'json' });
    if (!usageData) {
      usageData = { count: 0, date: today };
    }
    
    // Check if rate limit exceeded
    if (usageData.count >= DAILY_LIMIT) {
      const tomorrow = new Date();
      tomorrow.setDate(tomorrow.getDate() + 1);
      tomorrow.setHours(0, 0, 0, 0);
      
      console.log(`Rate limit exceeded for IP ${clientIP}: ${usageData.count}/${DAILY_LIMIT}`);
      
      return new Response(JSON.stringify({
        rateLimited: true,
        error: `Daily extraction limit reached. You can extract up to ${DAILY_LIMIT} pages per day.`,
        resetTime: tomorrow.getTime(),
        limit: DAILY_LIMIT,
        used: usageData.count,
        resetIn: Math.ceil((tomorrow.getTime() - Date.now()) / 1000 / 60), // minutes until reset
        success: false,
        metadata: {
          warning: "Rate limit exceeded. Please try again tomorrow.",
          terms: "By using this service, you agree not to violate any website's terms of service."
        }
      }), {
        status: 429,
        headers: { 
          'Content-Type': 'application/json',
          'X-RateLimit-Limit': DAILY_LIMIT.toString(),
          'X-RateLimit-Remaining': '0',
          'X-RateLimit-Reset': tomorrow.getTime().toString(),
          ...cors 
        },
      });
    }
    
    // Increment usage counter (will be saved after successful extraction)
    usageData.count++;
    
    // Store updated usage after request completes
    event.waitUntil(
      event.env.FAQ_RATE_LIMITS.put(rateLimitKey, JSON.stringify(usageData), {
        expirationTtl: 86400 // 24 hours
      })
    );
    
    // Add rate limit headers to response
    const remaining = DAILY_LIMIT - usageData.count;
    cors['X-RateLimit-Limit'] = DAILY_LIMIT.toString();
    cors['X-RateLimit-Remaining'] = Math.max(0, remaining).toString();
    
  } catch (kvError) {
    console.error('KV rate limit error:', kvError);
    // Continue without rate limiting if KV fails
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
    
    // Keep origin/referer for logging
    const requestOrigin = origin || referer || 'unknown origin';
    
    // Log the extraction request
    console.log(`FAQ extraction requested: ${url} from ${requestOrigin} at ${new Date().toISOString()}`);
    
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
    const schemaTypesFound = [];
    const warnings = [];
    const processing = {
      questionsWithHtmlStripped: 0,
      answersWithHtmlSanitized: 0,
      truncatedAnswers: 0,
      imagesProcessed: 0,
      brokenImages: 0,
      unverifiedImages: 0,
      relativeUrlsFixed: 0,
      dataUrisRejected: 0
    };
    
    // 1) Try Enhanced JSON-LD
    try {
      const { faqs, metadata } = await extractEnhancedJsonLd(root, targetUrl.href, processing);
      if (faqs.length > 0) {
        allFaqs = allFaqs.concat(faqs);
        schemaTypesFound.push('JSON-LD');
        if (metadata.warnings) warnings.push(...metadata.warnings);
      }
    } catch (e) {
      console.error('Enhanced JSON-LD extraction failed:', e);
    }
    
    // 2) Try Enhanced Microdata
    try {
      const { faqs, metadata } = await extractEnhancedMicrodata(root, targetUrl.href, processing);
      if (faqs.length > 0) {
        allFaqs = allFaqs.concat(faqs);
        schemaTypesFound.push('Microdata');
        if (metadata.warnings) warnings.push(...metadata.warnings);
      }
    } catch (e) {
      console.error('Enhanced Microdata extraction failed:', e);
    }
    
    // 3) Try Enhanced RDFa
    try {
      const { faqs, metadata } = await extractEnhancedRdfa(root, targetUrl.href, processing);
      if (faqs.length > 0) {
        allFaqs = allFaqs.concat(faqs);
        schemaTypesFound.push('RDFa');
        if (metadata.warnings) warnings.push(...metadata.warnings);
      }
    } catch (e) {
      console.error('Enhanced RDFa extraction failed:', e);
    }
    
    // Deduplicate and limit
    allFaqs = dedupeEnhanced(allFaqs);
    
    // Limit to 50 FAQs
    if (allFaqs.length > 50) {
      allFaqs = allFaqs.slice(0, 50);
      warnings.push('Limited to first 50 FAQs (found ' + allFaqs.length + ')');
    }
    
    // Build warnings from processing stats
    if (processing.questionsWithHtmlStripped > 0) {
      warnings.push(`${processing.questionsWithHtmlStripped} questions had HTML markup removed`);
    }
    if (processing.truncatedAnswers > 0) {
      warnings.push(`${processing.truncatedAnswers} answers were truncated to 5000 characters`);
    }
    if (processing.brokenImages > 0) {
      warnings.push(`${processing.brokenImages} images were unreachable`);
    }
    if (processing.unverifiedImages > 0) {
      warnings.push(`${processing.unverifiedImages} images could not be verified`);
    }
    if (processing.dataUrisRejected > 0) {
      warnings.push(`${processing.dataUrisRejected} embedded images were too large`);
    }
    
    if (allFaqs.length > 0) {
      console.log(`Successfully extracted ${allFaqs.length} FAQs from ${url}`);
      return new Response(JSON.stringify({
        success: true,
        source: url,
        faqs: allFaqs,
        metadata: { 
          extractionMethod: 'enhanced-html-parser', 
          totalExtracted: allFaqs.length, 
          title: title,
          processing: processing,
          warnings: warnings,
          schemaTypes: schemaTypesFound,
          hasImages: processing.imagesProcessed > 0,
          imageCount: processing.imagesProcessed,
          brokenImages: processing.brokenImages,
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
          warnings: ["FAQ schema detected but could not be parsed"],
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
        warnings: [],
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

// Enhanced JSON-LD extraction with preprocessing
async function extractEnhancedJsonLd(root, baseUrl, processing) {
  const faqs = [];
  const warnings = [];
  const scripts = root.querySelectorAll('script[type="application/ld+json"]');
  
  for (const script of scripts) {
    try {
      // Preprocess to handle comments and common issues
      let content = script.innerHTML
        .replace(/^\s*\/\/.*$/gm, '')     // Remove // comments
        .replace(/\/\*[\s\S]*?\*\//g, '') // Remove /* */ comments
        .replace(/[\u0000-\u001F\u007F-\u009F]/g, '') // Remove control chars
        .replace(/,\s*([}\]])/g, '$1')    // Remove trailing commas
        .trim();
      
      // Try to parse
      const data = JSON.parse(content);
      const arr = Array.isArray(data) ? data : [data];
      
      for (const obj of arr) {
        await traverseEnhancedLd(obj, faqs, baseUrl, processing);
      }
    } catch (e) {
      console.warn('Failed to parse JSON-LD:', e.message);
    }
  }
  
  return { faqs, metadata: { warnings } };
}

// Enhanced traversal for complex JSON-LD structures
async function traverseEnhancedLd(obj, out, baseUrl, processing, depth = 0) {
  if (!obj || typeof obj !== 'object' || depth > 5) return;
  
  const type = obj['@type'];
  
  // Check if this is or contains FAQPage
  if ((Array.isArray(type) ? type.includes('FAQPage') : type === 'FAQPage') ||
      (obj.mainEntity && obj.mainEntity['@type'] === 'FAQPage')) {
    
    // Find the FAQ content
    let faqContent = obj;
    if (obj.mainEntity && obj.mainEntity['@type'] === 'FAQPage') {
      faqContent = obj.mainEntity;
    }
    
    let mainEntity = faqContent.mainEntity || faqContent['mainEntity'] || faqContent.hasPart;
    if (mainEntity) {
      mainEntity = Array.isArray(mainEntity) ? mainEntity : [mainEntity];
      
      for (const q of mainEntity) {
        if (!q['@type'] || !q['@type'].includes('Question')) continue;
        
        // Process question
        const rawQuestion = q.name || q.question || '';
        const processedQuestion = processQuestion(rawQuestion, processing);
        if (!processedQuestion) continue;
        
        // Extract answer - try multiple properties
        let rawAnswer = '';
        const accepted = q.acceptedAnswer;
        const suggested = q.suggestedAnswer;
        
        if (accepted) {
          rawAnswer = typeof accepted === 'string' ? accepted : 
                     (accepted.text || accepted.answerText || accepted.description || '');
        } else if (suggested && suggested.length > 0) {
          const firstSuggested = suggested[0];
          rawAnswer = typeof firstSuggested === 'string' ? firstSuggested :
                     (firstSuggested.text || firstSuggested.answerText || '');
        }
        
        if (!rawAnswer) continue;
        
        // Process answer with full sanitization and image handling
        const processedAnswer = await processAnswer(rawAnswer, baseUrl, processing);
        
        // Extract ID/anchor
        let id = q['@id'] || q.id || q.url || null;
        if (id && id.includes('#')) {
          id = id.split('#').pop();
        }
        if (id) {
          id = sanitizeAnchor(id);
        }
        
        out.push({ 
          question: processedQuestion,
          answer: processedAnswer,
          id: id
        });
      }
    }
  }
  
  // Traverse nested structures
  if (obj['@graph'] && Array.isArray(obj['@graph'])) {
    for (const item of obj['@graph']) {
      await traverseEnhancedLd(item, out, baseUrl, processing, depth + 1);
    }
  }
  
  // Check for nested WebPage > mainEntity patterns
  if (obj.mainEntity && depth < 3) {
    await traverseEnhancedLd(obj.mainEntity, out, baseUrl, processing, depth + 1);
  }
}

// Enhanced Microdata extraction
async function extractEnhancedMicrodata(root, baseUrl, processing) {
  const faqs = [];
  const warnings = [];
  
  // First try FAQPage containers
  const faqPages = root.querySelectorAll('[itemscope][itemtype*="FAQPage"]');
  for (const faqPage of faqPages) {
    const questions = faqPage.querySelectorAll('[itemscope][itemtype*="Question"]');
    for (const q of questions) {
      await processMicrodataQuestion(q, faqs, baseUrl, processing);
    }
  }
  
  // Also try standalone Questions
  const standaloneQuestions = root.querySelectorAll('[itemscope][itemtype*="Question"]:not([itemtype*="FAQPage"] [itemscope][itemtype*="Question"])');
  for (const q of standaloneQuestions) {
    await processMicrodataQuestion(q, faqs, baseUrl, processing);
  }
  
  return { faqs, metadata: { warnings } };
}

async function processMicrodataQuestion(questionEl, faqs, baseUrl, processing) {
  // Get ID
  const id = sanitizeAnchor(
    questionEl.getAttribute('id') || 
    questionEl.getAttribute('itemid')?.split('#').pop() || 
    null
  );
  
  // Get question text - try multiple selectors
  let rawQuestion = '';
  const nameEl = questionEl.querySelector('[itemprop="name"]');
  if (nameEl) {
    rawQuestion = nameEl.textContent || nameEl.getAttribute('content') || '';
  }
  
  const processedQuestion = processQuestion(rawQuestion, processing);
  if (!processedQuestion) return;
  
  // Get answer - try multiple approaches
  let rawAnswer = '';
  
  // Direct text property
  const directTextEl = questionEl.querySelector('[itemprop="text"]');
  if (directTextEl) {
    rawAnswer = directTextEl.innerHTML;
  } else {
    // Inside acceptedAnswer
    const acceptedAnswerEl = questionEl.querySelector('[itemprop="acceptedAnswer"]');
    if (acceptedAnswerEl) {
      const textEl = acceptedAnswerEl.querySelector('[itemprop="text"]');
      if (textEl) {
        rawAnswer = textEl.innerHTML;
      } else {
        // Sometimes the acceptedAnswer itself contains the text
        rawAnswer = acceptedAnswerEl.innerHTML;
      }
    }
  }
  
  if (!rawAnswer) {
    // Try suggestedAnswer as fallback
    const suggestedEl = questionEl.querySelector('[itemprop="suggestedAnswer"] [itemprop="text"]');
    if (suggestedEl) {
      rawAnswer = suggestedEl.innerHTML;
    }
  }
  
  if (!rawAnswer) return;
  
  const processedAnswer = await processAnswer(rawAnswer, baseUrl, processing);
  
  faqs.push({
    question: processedQuestion,
    answer: processedAnswer,
    id: id
  });
}

// Enhanced RDFa extraction
async function extractEnhancedRdfa(root, baseUrl, processing) {
  const faqs = [];
  const warnings = [];
  
  // Try FAQPage containers first
  const faqPages = root.querySelectorAll('[typeof*="FAQPage"], [typeof*="https://schema.org/FAQPage"]');
  for (const faqPage of faqPages) {
    const questions = faqPage.querySelectorAll('[typeof*="Question"]');
    for (const q of questions) {
      await processRdfaQuestion(q, faqs, baseUrl, processing);
    }
  }
  
  // Also try standalone Questions
  const standaloneQuestions = root.querySelectorAll('[typeof*="Question"]:not([typeof*="FAQPage"] [typeof*="Question"])');
  for (const q of standaloneQuestions) {
    await processRdfaQuestion(q, faqs, baseUrl, processing);
  }
  
  return { faqs, metadata: { warnings } };
}

async function processRdfaQuestion(questionEl, faqs, baseUrl, processing) {
  // Get ID
  const id = sanitizeAnchor(
    questionEl.getAttribute('id') || 
    questionEl.getAttribute('resource')?.split('#').pop() ||
    questionEl.getAttribute('about')?.split('#').pop() ||
    null
  );
  
  // Get question text
  const nameEl = questionEl.querySelector('[property="name"], [property="schema:name"]');
  if (!nameEl) return;
  const rawQuestion = nameEl.textContent || nameEl.getAttribute('content') || '';
  
  const processedQuestion = processQuestion(rawQuestion, processing);
  if (!processedQuestion) return;
  
  // Get answer - try multiple selectors
  let rawAnswer = '';
  const textEl = questionEl.querySelector('[property="text"], [property="schema:text"], [property="acceptedAnswer"] [property="text"]');
  if (textEl) {
    rawAnswer = textEl.innerHTML;
  }
  
  if (!rawAnswer) return;
  
  const processedAnswer = await processAnswer(rawAnswer, baseUrl, processing);
  
  faqs.push({
    question: processedQuestion,
    answer: processedAnswer,
    id: id
  });
}

// Process question text
function processQuestion(raw, processing) {
  if (!raw) return '';
  
  // Decode HTML entities
  raw = decodeHtmlEntities(raw);
  
  // Check if contains HTML
  if (/<[^>]+>/.test(raw)) {
    processing.questionsWithHtmlStripped++;
  }
  
  // Strip all HTML tags
  raw = raw.replace(/<[^>]+>/g, '');
  
  // Normalize whitespace
  raw = raw.replace(/\s+/g, ' ').trim();
  
  // Limit length
  if (raw.length > 300) {
    // Try to cut at word boundary
    raw = raw.substring(0, 300);
    const lastSpace = raw.lastIndexOf(' ');
    if (lastSpace > 250) {
      raw = raw.substring(0, lastSpace) + '...';
    }
  }
  
  return raw;
}

// Process answer with sanitization and image handling
async function processAnswer(raw, baseUrl, processing) {
  if (!raw) return '';
  
  processing.answersWithHtmlSanitized++;
  
  // First decode entities
  raw = decodeHtmlEntities(raw);
  
  // Create temporary DOM for processing
  const tempDiv = document.createElement('div');
  tempDiv.innerHTML = raw;
  
  // Remove dangerous elements
  const dangerousTags = ['script', 'style', 'iframe', 'object', 'embed', 'form', 'input', 'button'];
  dangerousTags.forEach(tag => {
    const elements = tempDiv.querySelectorAll(tag);
    elements.forEach(el => el.remove());
  });
  
  // Remove event handlers
  const allElements = tempDiv.querySelectorAll('*');
  allElements.forEach(el => {
    // Remove all attributes starting with 'on'
    Array.from(el.attributes).forEach(attr => {
      if (attr.name.startsWith('on') || attr.value.includes('javascript:')) {
        el.removeAttribute(attr.name);
      }
    });
  });
  
  // Process links - make relative URLs absolute
  const links = tempDiv.querySelectorAll('a');
  let relativeUrlsFixed = 0;
  links.forEach(link => {
    const href = link.getAttribute('href');
    if (href && !href.startsWith('http') && !href.startsWith('#') && !href.startsWith('mailto:')) {
      try {
        const absolute = new URL(href, baseUrl).href;
        link.setAttribute('href', absolute);
        relativeUrlsFixed++;
      } catch (e) {
        // Invalid URL, remove href
        link.removeAttribute('href');
      }
    }
  });
  processing.relativeUrlsFixed += relativeUrlsFixed;
  
  // Process images
  const images = tempDiv.querySelectorAll('img');
  processing.imagesProcessed += images.length;
  
  // Process up to 10 images with verification
  let imageCheckCount = 0;
  for (const img of images) {
    let src = img.getAttribute('src');
    
    if (!src) {
      img.remove();
      continue;
    }
    
    // Handle data URIs
    if (src.startsWith('data:')) {
      if (src.length > 100000) { // 100KB limit
        img.setAttribute('src', '#');
        img.setAttribute('alt', img.getAttribute('alt') || 'Image too large to display');
        img.setAttribute('data-error', 'embedded-image-too-large');
        processing.dataUrisRejected++;
        processing.brokenImages++;
      }
      continue;
    }
    
    // Fix relative URLs
    if (!src.startsWith('http')) {
      try {
        // Handle protocol-relative URLs
        if (src.startsWith('//')) {
          src = 'https:' + src;
        } else {
          src = new URL(src, baseUrl).href;
        }
        img.setAttribute('src', src);
        processing.relativeUrlsFixed++;
      } catch (e) {
        img.setAttribute('data-broken', 'true');
        img.setAttribute('alt', img.getAttribute('alt') || 'Image unavailable');
        processing.brokenImages++;
        continue;
      }
    }
    
    // Add lazy loading
    img.setAttribute('loading', 'lazy');
    
    // Add alt text if missing
    if (!img.getAttribute('alt')) {
      img.setAttribute('alt', 'FAQ image');
    }
    
    // Verify image availability (limit to 10 checks)
    if (imageCheckCount < 10) {
      imageCheckCount++;
      try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 2000); // 2 second timeout
        
        const response = await fetch(src, {
          method: 'HEAD',
          signal: controller.signal,
          // Note: no-cors mode means we can't read response, but that's OK
          mode: 'no-cors'
        });
        
        clearTimeout(timeoutId);
        
        // With no-cors, we can't actually check if it's OK, so mark as unverified
        img.setAttribute('data-verified', 'unverified');
        processing.unverifiedImages++;
        
      } catch (e) {
        // Timeout or network error
        if (e.name === 'AbortError') {
          img.setAttribute('data-verified', 'timeout');
          processing.unverifiedImages++;
        } else {
          img.setAttribute('data-broken', 'true');
          img.setAttribute('alt', img.getAttribute('alt') || 'Image unavailable');
          processing.brokenImages++;
        }
      }
    } else {
      // Skip verification for remaining images
      img.setAttribute('data-verified', 'skipped');
    }
  }
  
  // Clean up empty paragraphs and normalize
  const paragraphs = tempDiv.querySelectorAll('p');
  paragraphs.forEach(p => {
    if (!p.textContent.trim() && !p.querySelector('img')) {
      p.remove();
    }
  });
  
  // Get cleaned HTML
  let cleaned = tempDiv.innerHTML;
  
  // Final length check
  if (cleaned.length > 5000) {
    cleaned = cleaned.substring(0, 5000);
    // Try to close any open tags
    cleaned = cleaned.replace(/<[^>]*$/, '') + '... (truncated)';
    processing.truncatedAnswers++;
  }
  
  return cleaned;
}

// Sanitize anchor/ID
function sanitizeAnchor(id) {
  if (!id) return null;
  
  // Remove any dangerous characters
  return id
    .replace(/[^a-zA-Z0-9_-]/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '')
    .substring(0, 100);
}

// Decode HTML entities
function decodeHtmlEntities(text) {
  const entities = {
    '&amp;': '&',
    '&lt;': '<',
    '&gt;': '>',
    '&quot;': '"',
    '&#39;': "'",
    '&nbsp;': ' ',
    '&mdash;': '—',
    '&ndash;': '–',
    '&hellip;': '…',
    '&copy;': '©',
    '&reg;': '®',
    '&trade;': '™'
  };
  
  return text.replace(/&[#a-zA-Z0-9]+;/g, (match) => entities[match] || match);
}

// Enhanced deduplication
function dedupeEnhanced(arr) {
  const seen = new Map();
  const MAX_FAQS = 50;
  
  return arr.filter((faq, index) => {
    if (index >= MAX_FAQS) return false;
    
    if (!faq.question || !faq.answer) return false;
    if (faq.question.includes('${') || faq.answer.includes('${')) return false;
    
    // Create normalized key for comparison
    const key = faq.question.toLowerCase()
      .replace(/[^\w\s]/g, '')
      .replace(/\s+/g, ' ')
      .trim();
    
    if (seen.has(key)) {
      // Keep the one with an ID if duplicate
      const existing = seen.get(key);
      if (!existing.id && faq.id) {
        seen.set(key, faq);
      }
      return false;
    }
    
    seen.set(key, faq);
    return true;
  });
}