import { parse }           from 'node-html-parser';
import { load as cheerio } from 'cheerio';
/**
 * FAQ Schema Extraction Proxy Worker - Fixed for Unquoted Attributes
 * Handles both quoted and unquoted HTML attributes
 */
addEventListener('fetch', e => e.respondWith(handleRequest(e.request)));

async function handleRequest(request) {
  const cors = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, HEAD, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };
  
  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: cors });
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

  let resp;
  try {
    const u = new URL(url);
    u.searchParams.append('_cb', Date.now());
    resp = await fetch(u.toString(), {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Cache-Control': 'no-cache',
      },
      cf: { cacheTtl: 0, cacheEverything: false },
    });
  } catch (err) {
    return new Response(JSON.stringify({ 
      error: err.message, 
      success: false 
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...cors },
    });
  }
  
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
  const title = extractTitle(html);

  // 1) Try JSON-LD
  const jsonLdFaqs = extractJsonLd(html);
  if (jsonLdFaqs.length) {
    return createResponse(jsonLdFaqs, 'json-ld', title, url, cors);
  }

  // 2) Try Microdata with unquoted attribute support
  const microdataFaqs = extractMicrodata(html);
  if (microdataFaqs.length) {
    return createResponse(microdataFaqs, 'microdata', title, url, cors);
  }

  // 3) Try RDFa
  const rdfaFaqs = extractRdfa(html);
  if (rdfaFaqs.length) {
    return createResponse(rdfaFaqs, 'rdfa', title, url, cors);
  }

  // Check if markup exists
  const hasFaqMarkup = html.includes('schema.org/FAQPage') || 
                       html.includes('typeof="FAQPage"') ||
                       html.includes('typeof=FAQPage') ||
                       html.includes('"@type":"FAQPage"');
  
  if (hasFaqMarkup) {
    return new Response(JSON.stringify({
      success: false,
      source: url,
      error: "Page contains FAQ markup but extraction failed. The structure might be non-standard.",
      metadata: {
        title: title,
        extractionMethod: "failed"
      }
    }), {
      headers: { 'Content-Type': 'application/json', ...cors }
    });
  }

  // No FAQs found
  return new Response(JSON.stringify({
    success: false,
    source: url,
    faqs: [],
    metadata: { 
      extractionMethod: 'none', 
      title: title,
      message: "No FAQ schema markup found on this page"
    }
  }), { 
    headers: { 'Content-Type': 'application/json', ...cors } 
  });
}

function createResponse(faqs, method, title, url, cors) {
  return new Response(JSON.stringify({
    success: true,
    source: url,
    faqs: faqs,
    metadata: { 
      extractionMethod: method, 
      totalExtracted: faqs.length, 
      title: title 
    }
  }), { 
    headers: { 'Content-Type': 'application/json', ...cors } 
  });
}

function extractJsonLd(html) {
  const faqs = [];
  const re = /<script[^>]*type=["']?application\/ld\+json["']?[^>]*>([\s\S]*?)<\/script>/gi;
  let m;
  
  while ((m = re.exec(html))) {
    try {
      const data = JSON.parse(m[1].trim());
      const arr = Array.isArray(data) ? data : [data];
      arr.forEach(d => traverseLd(d, faqs));
    } catch (e) {
      // Skip invalid JSON
    }
  }
  
  return dedupe(faqs);
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

function extractMicrodata(html) {
  const faqs = [];
  
  // Pattern for unquoted attributes (like your HTML)
  const unquotedPattern = /<div\s+itemscope\s+itemprop=mainEntity\s+itemtype="[^"]*Question"[^>]*(?:\s+id=([^\s>]+))?[^>]*>([\s\S]*?)(?=<div\s+itemscope\s+itemprop=mainEntity|$)/gi;
  
  let match;
  while ((match = unquotedPattern.exec(html))) {
    const id = match[1] || null;
    const content = match[2];
    
    // Extract question - handle unquoted itemprop
    const qMatch = content.match(/<[^>]+itemprop=name[^>]*>([^<]+)<\/[^>]+>/i);
    
    // Extract answer - look for text inside acceptedAnswer
    let answer = '';
    const acceptedAnswerMatch = content.match(/<div\s+itemscope\s+itemprop=acceptedAnswer[^>]*>([\s\S]*?)<\/div>/i);
    if (acceptedAnswerMatch) {
      const answerContent = acceptedAnswerMatch[1];
      const textMatch = answerContent.match(/<div\s+itemprop=text[^>]*>([\s\S]*?)<\/div>/i);
      if (textMatch) {
        answer = textMatch[1].trim();
      }
    }
    
    if (qMatch && answer) {
      faqs.push({
        question: qMatch[1].trim(),
        answer: answer,
        id: id
      });
    }
  }
  
  // Fallback: Try with flexible attribute matching (both quoted and unquoted)
  if (faqs.length === 0) {
    // This pattern handles both quoted and unquoted attributes
    const flexiblePattern = /<div[^>]*\s+itemscope\s+[^>]*itemprop=["']?mainEntity["']?[^>]*itemtype=["']?[^"'\s]*Question["']?[^>]*>([\s\S]*?)(?=<div[^>]*\s+itemscope\s+[^>]*itemprop=["']?mainEntity["']?|$)/gi;
    
    while ((match = flexiblePattern.exec(html))) {
      const fullMatch = match[0];
      const content = match[1];
      
      // Extract ID
      let id = null;
      const idMatch = fullMatch.match(/\s+id=["']?([^"'\s>]+)["']?/i);
      if (idMatch) {
        id = idMatch[1];
      }
      
      // Extract question
      const qMatch = content.match(/<[^>]+itemprop=["']?name["']?[^>]*>([^<]+)<\/[^>]+>/i);
      
      // Extract answer
      let answer = '';
      const answerMatch = content.match(/<div[^>]+itemprop=["']?text["']?[^>]*>([\s\S]*?)<\/div>/i);
      if (answerMatch) {
        answer = answerMatch[1].trim();
      } else {
        // Try within acceptedAnswer
        const acceptedMatch = content.match(/<[^>]+itemprop=["']?acceptedAnswer["']?[^>]*>([\s\S]*?)<\/[^>]+>/i);
        if (acceptedMatch) {
          const textMatch = acceptedMatch[1].match(/<[^>]+itemprop=["']?text["']?[^>]*>([\s\S]*?)<\/[^>]+>/i);
          if (textMatch) {
            answer = textMatch[1].trim();
          }
        }
      }
      
      if (qMatch && answer) {
        faqs.push({
          question: qMatch[1].trim(),
          answer: answer,
          id: id
        });
      }
    }
  }
  
  return dedupe(faqs);
}

function extractRdfa(html) {
  const faqs = [];
  
  // Patterns that handle both quoted and unquoted attributes
  const patterns = [
    /<[^>]+typeof=["']?Question["']?[^>]*>([\s\S]*?)(?=<[^>]+typeof=["']?Question["']?|$)/gi,
    /<[^>]+property=["']?mainEntity["']?[^>]*typeof=["']?Question["']?[^>]*>([\s\S]*?)(?=<[^>]+property=["']?mainEntity["']?|$)/gi
  ];
  
  patterns.forEach(pattern => {
    let match;
    pattern.lastIndex = 0;
    
    while ((match = pattern.exec(html))) {
      const content = match[1];
      const fullBlock = match[0];
      
      // Extract ID
      let id = null;
      const idMatch = fullBlock.match(/(?:id|resource)=["']?([^"'\s>]+)["']?/i);
      if (idMatch) {
        id = idMatch[1];
        if (id.includes('#')) {
          id = id.split('#').pop();
        }
      }
      
      // Extract question
      const qMatch = content.match(/<[^>]+property=["']?name["']?[^>]*>([^<]+)<\/[^>]+>/i);
      
      // Extract answer
      const aMatch = content.match(/<[^>]+property=["']?text["']?[^>]*>([\s\S]*?)<\/[^>]+>/i);
      
      if (qMatch && aMatch) {
        faqs.push({
          question: qMatch[1].trim(),
          answer: aMatch[1].trim(),
          id: id
        });
      }
    }
  });
  
  return dedupe(faqs);
}

function extractTitle(html) {
  const m = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  return m ? m[1].trim() : '';
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
