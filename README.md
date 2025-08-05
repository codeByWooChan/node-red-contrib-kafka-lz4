# node-red-contrib-kafka-lz4

A Node-RED node that automatically processes Kafka messages with LZ4 compression support and corrupted JSON data recovery.

## Features

- **Auto-detection**: Automatically detects LZ4 compressed data, corrupted JSON, or regular data
- **LZ4 Decompression**: Seamlessly decompresses LZ4-compressed Kafka messages
- **Data Recovery**: Repairs corrupted JSON data with control characters and structural issues
- **Smart Processing**: Only compresses when efficient, otherwise cleans and returns original data
- **Kafka Optimized**: Designed specifically for Kafka message processing workflows

## Usage

1. Drag the **kafka lz4** node from the function palette into your flow
2. Connect it between your Kafka consumer and processing nodes
3. Configure output format if needed (Buffer, Base64, or Hex)
4. Deploy your flow

The node will automatically:
- Decompress LZ4-compressed messages
- Clean up corrupted JSON data
- Parse valid JSON into objects
- Handle regular data appropriately

## Input/Output

**Input**: `msg.payload` - Any data (Buffer, String, Object)
**Output**: 
- `msg.payload` - Processed data (Object, String, or Buffer)
- `msg.lz4` - Processing metadata (operation, sizes, format)

## Configuration

- **Output Format**: Choose Buffer, Base64, or Hex for compressed output
- **Compression Level**: Reserved for future use (1-9)

## Status Indicators

- 🟢 **Green dot**: Ready or data compression completed
- 🔵 **Blue dot**: LZ4 decompression or data cleanup completed
- 🟡 **Yellow ring**: Warning (no payload, processing failed)
- 🔴 **Red ring**: Operation failed

## Example Flow

```
[Kafka Consumer] → [kafka lz4] → [JSON Processing] → [Output]
```

The node sits between your Kafka consumer and data processing, automatically handling compression and data corruption issues.

## License

MIT - See [LICENSE](LICENSE) file for details.