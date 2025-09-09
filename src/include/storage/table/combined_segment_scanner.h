#pragma once

#include "storage/table/column_chunk_data.h"
#include "storage/table/segment_scanner.h"
namespace kuzu {
namespace storage {
// Scans all the segments into a single column chunk
struct CombinedSegmentScanner : public SegmentScanner {
    explicit CombinedSegmentScanner(ColumnChunkData& output) : output(output) {}

    void initSegment(common::offset_t, common::offset_t, scan_func_t scanFunc) override {
        scanFunc(output);
    }

    void writeToSegment(ColumnChunkData& data, [[maybe_unused]] common::idx_t segmentIdx,
        common::offset_t offsetInSegment, common::offset_t dstOffset) override {
        KU_ASSERT(segmentIdx == 0);
        output.write(&data, offsetInSegment, dstOffset, 1);
    }

    void rangeSegments(common::offset_t offsetInChunk, common::length_t length,
        const range_func_t& func) override {
        func(0, offsetInChunk, length, 0);
    }

    uint64_t getNumValues() override { return output.getNumValues(); }

    ColumnChunkData& output;
};
} // namespace storage
} // namespace kuzu
