const lz4 = require('lz4');

module.exports = function(RED) {
    function KafkaLZ4Node(config) {
        RED.nodes.createNode(this, config);
        const node = this;
        
        // Node configuration
        node.compressionLevel = config.compressionLevel || 1;
        node.outputFormat = config.outputFormat || 'buffer';
        
        // Initial status
        node.status({fill: "green", shape: "dot", text: "ready"});
        
        node.on('input', function(msg) {
            try {
                // Input data validation
                if (!msg.payload) {
                    node.warn("No payload found in message");
                    node.status({fill: "yellow", shape: "ring", text: "no payload"});
                    return;
                }
                
                let inputData;
                let isDecompression = false;
                
                // Analyze input data and determine processing mode
                if (Buffer.isBuffer(msg.payload)) {
                    inputData = msg.payload;
                    // Check LZ4 magic number
                    if (msg.payload.length > 4) {
                        const header = msg.payload.readUInt32LE(0);
                        if (header === 0x184D2204) {
                            isDecompression = true;
                        }
                    }
                } else if (typeof msg.payload === 'string') {
                    // String analysis - check if data is corrupted
                    const hasControlChars = /[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]/.test(msg.payload);
                    const hasCorruptedStructure = /[{}].*[^\w\s",:{}[\].-]+.*[{}]/.test(msg.payload);
                    
                    if (hasControlChars || hasCorruptedStructure) {
                        // Corrupted data - set cleanup mode
                        node.isCleanupMode = true;
                        inputData = msg.payload; // Use string as is
                    } else {
                        // Regular string or Base64 encoded compressed data
                        try {
                            const base64Decoded = Buffer.from(msg.payload, 'base64');
                            if (base64Decoded.length > 4) {
                                const header = base64Decoded.readUInt32LE(0);
                                if (header === 0x184D2204) {
                                    isDecompression = true;
                                    inputData = base64Decoded;
                                }
                            }
                        } catch (e) {
                            // Not Base64, treat as regular string
                        }
                        
                        if (!isDecompression && !node.isCleanupMode) {
                            inputData = Buffer.from(msg.payload, 'utf8');
                        }
                    }
                } else if (typeof msg.payload === 'object') {
                    inputData = Buffer.from(JSON.stringify(msg.payload), 'utf8');
                } else {
                    inputData = Buffer.from(String(msg.payload), 'utf8');
                }
                
                let outputPayload;
                let outputMsg;
                
                if (node.isCleanupMode) {
                    // Corrupted data cleanup mode
                    outputPayload = node.processCorruptedData(inputData);
                    outputMsg = {
                        ...msg,
                        payload: outputPayload,
                        lz4: {
                            operation: 'cleanup',
                            originalSize: inputData.length
                        }
                    };
                    node.status({fill: "blue", shape: "dot", text: "cleaned data"});
                    node.isCleanupMode = false; // Reset
                } else if (isDecompression) {
                    // Perform LZ4 decompression - try multiple methods
                    let decompressedData = null;
                    let decompressedText = '';
                    
                    // Method 1: Direct decoding
                    try {
                        decompressedData = lz4.decode(inputData);
                        decompressedText = decompressedData.toString('utf8');
                    } catch (e) {
                        // Method 2: Try decoding with header skip
                        for (let offset = 1; offset <= 20 && offset < inputData.length; offset++) {
                            try {
                                decompressedData = lz4.decode(inputData.slice(offset));
                                decompressedText = decompressedData.toString('utf8');
                                break;
                            } catch (skipError) {
                                continue;
                            }
                        }
                    }
                    
                    if (decompressedData) {
                        outputPayload = node.processCorruptedData(decompressedText);
                        outputMsg = {
                            ...msg,
                            payload: outputPayload,
                            lz4: {
                                operation: 'decompress',
                                originalSize: inputData.length,
                                decompressedSize: decompressedData.length,
                                format: 'decompressed'
                            }
                        };
                        node.status({
                            fill: "blue", 
                            shape: "dot", 
                            text: `decompressed (${inputData.length}→${decompressedData.length})`
                        });
                    } else {
                        // All decompression methods failed
                        node.warn("All LZ4 decompression methods failed, returning original data");
                        outputPayload = inputData;
                        outputMsg = {
                            ...msg,
                            payload: outputPayload,
                            lz4: {
                                operation: 'decompress_failed',
                                error: 'All decompression methods failed'
                            }
                        };
                        node.status({fill: "yellow", shape: "ring", text: "decompress failed"});
                    }
                } else {
                    // Perform LZ4 compression
                    const compressedData = lz4.encode(inputData);
                    
                    // Compression statistics
                    const originalSize = inputData.length;
                    const compressedSize = compressedData.length;
                    const compressionRatio = ((originalSize - compressedSize) / originalSize * 100).toFixed(2);
                    
                    // Check compression efficiency - return cleaned original if poor compression
                    if (parseFloat(compressionRatio) < 5) {
                        outputPayload = node.processCorruptedData(inputData.toString('utf8'));
                        outputMsg = {
                            ...msg,
                            payload: outputPayload,
                            lz4: {
                                operation: 'cleaned',
                                originalSize: originalSize
                            }
                        };
                        node.status({fill: "blue", shape: "dot", text: "cleaned data"});
                    } else {
                        // Return compressed data only if compression is efficient
                        switch (node.outputFormat) {
                            case 'base64':
                                outputPayload = compressedData.toString('base64');
                                break;
                            case 'hex':
                                outputPayload = compressedData.toString('hex');
                                break;
                            case 'buffer':
                            default:
                                outputPayload = compressedData;
                                break;
                        }
                        
                        outputMsg = {
                            ...msg,
                            payload: outputPayload,
                            lz4: {
                                operation: 'compress',
                                originalSize: originalSize,
                                compressedSize: compressedSize,
                                compressionRatio: compressionRatio + '%',
                                format: node.outputFormat
                            }
                        };
                        
                        node.status({
                            fill: "green", 
                            shape: "dot", 
                            text: `${compressionRatio}% saved (${originalSize}→${compressedSize})`
                        });
                    }
                }
                
                // Send message
                node.send(outputMsg);
                
            } catch (error) {
                node.error("LZ4 operation failed: " + error.message, msg);
                node.status({fill: "red", shape: "ring", text: "operation failed"});
            }
        });
        
        // Process corrupted data (unified function for cleanup, decompression, and compression)
        node.processCorruptedData = function(data) {
            try {
                const cleanText = node.cleanDecompressedText(data);
                const recoveredText = node.recoverJsonText(cleanText);
                return node.tryParseJson(recoveredText) || 
                       node.tryParseJson(cleanText) || 
                       recoveredText || cleanText;
            } catch (error) {
                return data;
            }
        };
        
        // Text cleanup function
        node.cleanDecompressedText = function(text) {
            // 1. Remove control characters
            let cleaned = text.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]/g, '');
            
            // 2. Remove invalid UTF-8 sequences
            cleaned = cleaned.replace(/[\uFFFD]/g, '');
            
            // 3. Clean up consecutive whitespace
            cleaned = cleaned.replace(/\s+/g, ' ');
            
            return cleaned.trim();
        };
        
        // JSON text recovery function
        node.recoverJsonText = function(text) {
            try {
                const jsonStart = text.indexOf('{');
                const jsonEnd = text.lastIndexOf('}');
                
                if (jsonStart === -1 || jsonEnd === -1 || jsonStart >= jsonEnd) {
                    return text;
                }
                
                let jsonText = text.substring(jsonStart, jsonEnd + 1);
                
                // 0. Fix JSON start structure (remove invalid characters after {)
                jsonText = node.fixJsonStart(jsonText);
                
                // 0.1. Fix JSON end structure (remove invalid characters before })
                jsonText = node.fixJsonEnd(jsonText);
                
                // 1. Remove control characters and corrupted characters
                jsonText = jsonText.replace(/[^\x20-\x7E\s{}[\]":,.-]/g, '');
                
                // 2. Clean corrupted string values (weird characters between quotes)
                jsonText = jsonText.replace(/"([^"]*)[^\w\s",:{}[\].-]+([^"]*)"/g, '"$1$2"');
                
                // 3. Fix invalid structure
                jsonText = jsonText.replace(/,\s*}/g, '}');
                jsonText = jsonText.replace(/,\s*]/g, ']');
                jsonText = jsonText.replace(/}\s*{/g, '}, {');
                
                // 4. Clean up consecutive whitespace and newlines
                jsonText = jsonText.replace(/\s+/g, ' ').trim();
                
                return jsonText;
            } catch (error) {
                return text;
            }
        };
        
        // Fix JSON start structure function
        node.fixJsonStart = function(jsonText) {
            try {
                // Check if it starts correctly with {" or { "
                if (/^{\s*"/.test(jsonText)) {
                    return jsonText; // Already in correct format
                }
                
                // Handle cases with invalid characters after { or { (space)
                // Patterns: { 7e{", { $:b{", { {", {T1{", etc.
                const patterns = [
                    // Pattern 1: { + invalid_chars + {"
                    /^{\s*([^"{}]+)({.*)/,
                    // Pattern 2: {{ or {{{ etc.
                    /^{+(.*)$/,
                    // Pattern 3: { + space + invalid + {"
                    /^{\s+([^"{}]+)({.*)/
                ];
                
                for (const pattern of patterns) {
                    const match = jsonText.match(pattern);
                    if (match) {
                        const remainingPart = match[2] || match[1];
                        
                        // Try different candidates
                        const candidates = [
                            remainingPart, // Direct remaining part
                            '{' + remainingPart, // Add single bracket
                            remainingPart.startsWith('"') ? '{' + remainingPart : null // Add bracket if starts with quote
                        ].filter(Boolean);
                        
                        for (const candidate of candidates) {
                            if (candidate.startsWith('{') && candidate.includes('"')) {
                                // Basic validation: check if it looks like JSON
                                try {
                                    // Try a simple structure check
                                    if (node.looksLikeJson(candidate)) {
                                        return candidate;
                                    }
                                } catch (e) {
                                    continue;
                                }
                            }
                        }
                    }
                }
                
                return jsonText;
            } catch (error) {
                return jsonText;
            }
        };
        
        // Quick check if text looks like JSON structure
        node.looksLikeJson = function(text) {
            try {
                if (!text || !text.startsWith('{') || !text.includes('"')) {
                    return false;
                }
                
                // Check for basic JSON patterns
                const hasKeyValuePairs = /"[^"]*"\s*:\s*[^,}]+/.test(text);
                const hasValidEnding = text.trim().endsWith('}') || text.trim().endsWith(']');
                
                return hasKeyValuePairs && hasValidEnding;
            } catch (error) {
                return false;
            }
        };
        
        // Fix JSON end structure function
        node.fixJsonEnd = function(jsonText) {
            try {
                // Simple case: check for excessive closing brackets like }}}, ]}}, etc.
                const excessiveBrackets = /^(.*["\]\}])\s*([\]\}]{2,})$/;
                const match = jsonText.match(excessiveBrackets);
                
                if (match) {
                    const mainPart = match[1];
                    
                    // Try different valid single endings
                    const candidates = [
                        mainPart + '}',
                        mainPart + ']',
                        mainPart + ']}',
                        mainPart + '}}'
                    ];
                    
                    for (const candidate of candidates) {
                        if (node.isValidJsonStructure(candidate)) {
                            return candidate;
                        }
                    }
                }
                
                // More complex patterns: handle invalid characters before closing brackets
                const patterns = [
                    // Pattern 1: "value"}} -> "value"}
                    /^(.*"[^"]*")\s*}+$/,
                    // Pattern 2: ...]} + extra }
                    /^(.*[\]\}])\s*[\]\}]+$/,
                    // Pattern 3: ...} + invalid chars + }
                    /(.*["\]\}])([^"\]\}\s]+)}+$/
                ];
                
                for (const pattern of patterns) {
                    const patternMatch = jsonText.match(pattern);
                    if (patternMatch) {
                        const mainPart = patternMatch[1];
                        
                        // Try different valid endings
                        const candidates = [
                            mainPart + '}',
                            mainPart + ']',
                            mainPart + ']}',
                            mainPart
                        ];
                        
                        for (const candidate of candidates) {
                            if (node.isValidJsonStructure(candidate)) {
                                return candidate;
                            }
                        }
                    }
                }
                
                return jsonText;
            } catch (error) {
                return jsonText;
            }
        };
        
        // Check if text has valid JSON structure
        node.isValidJsonStructure = function(text) {
            try {
                // Basic structure checks
                if (!text || typeof text !== 'string') {
                    return false;
                }
                
                // Must start with { and end with } or ]
                if (!text.trim().startsWith('{')) {
                    return false;
                }
                
                const trimmed = text.trim();
                if (!trimmed.endsWith('}') && !trimmed.endsWith(']')) {
                    return false;
                }
                
                // Check bracket balance
                let braceCount = 0;
                let bracketCount = 0;
                let inString = false;
                let escaped = false;
                
                for (let i = 0; i < trimmed.length; i++) {
                    const char = trimmed[i];
                    
                    if (escaped) {
                        escaped = false;
                        continue;
                    }
                    
                    if (char === '\\') {
                        escaped = true;
                        continue;
                    }
                    
                    if (char === '"') {
                        inString = !inString;
                        continue;
                    }
                    
                    if (!inString) {
                        if (char === '{') braceCount++;
                        else if (char === '}') braceCount--;
                        else if (char === '[') bracketCount++;
                        else if (char === ']') bracketCount--;
                    }
                }
                
                // Brackets should be balanced
                return braceCount === 0 && bracketCount === 0;
                
            } catch (error) {
                return false;
            }
        };
        
        // Try to parse JSON with multiple attempts
        node.tryParseJson = function(text) {
            if (!text || typeof text !== 'string') return null;
            
            const attempts = [
                // Direct parsing
                () => JSON.parse(text),
                
                // Fix common issues
                () => {
                    let fixed = text
                        .replace(/,(\s*[}\]])/g, '$1')  // trailing commas
                        .replace(/([{,]\s*)([a-zA-Z_][a-zA-Z0-9_]*)\s*:/g, '$1"$2":')  // unquoted keys
                        .replace(/'/g, '"')  // single to double quotes
                        .trim();
                    return JSON.parse(fixed);
                },
                
                // Extract JSON from mixed content
                () => {
                    const match = text.match(/{.*}/s);
                    return match ? JSON.parse(match[0]) : null;
                }
            ];
            
            for (const attempt of attempts) {
                try {
                    const result = attempt();
                    if (result !== null) return result;
                } catch (e) {
                    continue;
                }
            }
            
            return null;
        };
        
        node.on('close', function() {
            node.status({});
        });
    }
    
    RED.nodes.registerType("kafka-lz4", KafkaLZ4Node);
};