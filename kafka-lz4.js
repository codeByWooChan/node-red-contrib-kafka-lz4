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
                    try {
                        let cleanText = node.cleanDecompressedText(inputData);
                        const recoveredText = node.recoverJsonText(cleanText);
                        
                        try {
                            const jsonData = JSON.parse(recoveredText);
                            outputPayload = jsonData;
                        } catch (e) {
                            outputPayload = recoveredText || cleanText;
                        }
                        
                        outputMsg = {
                            ...msg,
                            payload: outputPayload,
                            lz4: {
                                operation: 'cleanup',
                                originalSize: inputData.length
                            }
                        };
                        
                        node.status({
                            fill: "blue", 
                            shape: "dot", 
                            text: `cleaned data`
                        });
                        
                    } catch (error) {
                        outputPayload = inputData;
                        outputMsg = {
                            ...msg,
                            payload: outputPayload,
                            lz4: {
                                operation: 'cleanup_failed',
                                error: error.message
                            }
                        };
                        node.status({fill: "yellow", shape: "ring", text: "cleanup failed"});
                    }
                    
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
                        // Smart text cleanup
                        let cleanText = node.cleanDecompressedText(decompressedText);
                        
                        // Try JSON parsing
                        try {
                            const jsonData = JSON.parse(cleanText);
                            outputPayload = jsonData;
                        } catch (e) {
                            // If JSON parsing fails, try text recovery
                            const recoveredText = node.recoverJsonText(cleanText);
                            try {
                                const jsonData = JSON.parse(recoveredText);
                                outputPayload = jsonData;
                            } catch (e2) {
                                outputPayload = recoveredText || cleanText;
                            }
                        }
                        
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
                        // Clean original data
                        let cleanedPayload = inputData.toString('utf8');
                        if (/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]/.test(cleanedPayload)) {
                            cleanedPayload = node.cleanDecompressedText(cleanedPayload);
                            const recoveredText = node.recoverJsonText(cleanedPayload);
                            try {
                                const jsonData = JSON.parse(recoveredText);
                                outputPayload = jsonData;
                            } catch (e) {
                                outputPayload = recoveredText || cleanedPayload;
                            }
                        } else {
                            try {
                                const jsonData = JSON.parse(cleanedPayload);
                                outputPayload = jsonData;
                            } catch (e) {
                                outputPayload = cleanedPayload;
                            }
                        }
                        
                        outputMsg = {
                            ...msg,
                            payload: outputPayload,
                            lz4: {
                                operation: 'cleaned',
                                originalSize: originalSize
                            }
                        };
                        
                        node.status({
                            fill: "blue", 
                            shape: "dot", 
                            text: `cleaned data`
                        });
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
        
        node.on('close', function() {
            node.status({});
        });
    }
    
    RED.nodes.registerType("kafka-lz4", KafkaLZ4Node);
};