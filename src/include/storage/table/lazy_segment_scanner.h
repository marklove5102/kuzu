#pragma once

#include "storage/table/column_chunk.h"
#include "storage/table/segment_scanner.h"
namespace kuzu {
namespace storage {
// Separately scans each segment
// Avoids scanning a segment unless a call to writeToSegment() is made for the current segment
struct LazySegmentScanner : public SegmentScanner {
    struct SegmentData {
        std::unique_ptr<ColumnChunkData> segmentData;
        common::offset_t offsetInChunk;
        common::offset_t length;
        scan_func_t scanFunc;
    };

    struct Iterator {
        Iterator operator++() {
            KU_ASSERT(segmentIdx < scanner.segments.size());
            ++offsetInSegment;
            if (offsetInSegment >= scanner.segments[segmentIdx].length) {
                offsetInSegment = 0;
                ++segmentIdx;
            }
            return *this;
        }

        SegmentData& operator*() { return scanner.segments[segmentIdx]; }

        bool isNewSegment() const { return offsetInSegment == 0; }

        common::idx_t segmentIdx;
        common::offset_t offsetInSegment;
        LazySegmentScanner& scanner;
    };

    LazySegmentScanner(MemoryManager& mm, common::LogicalType columnType, bool enableCompression)
        : numValues(0), mm(mm), columnType(std::move(columnType)),
          enableCompression(enableCompression) {}

    Iterator begin() { return Iterator{0, 0, *this}; }

    void initSegment(common::offset_t dstOffset, common::offset_t segmentLength,
        scan_func_t newScanFunc) override {
        segments.emplace_back(nullptr, dstOffset, segmentLength, std::move(newScanFunc));
        numValues += segmentLength;
    }

    void writeToSegment(ColumnChunkData& data, common::idx_t segmentIdx,
        common::offset_t offsetInSegment, common::offset_t dstOffset) override {
        initScannedSegmentIfNeeded(segmentIdx);
        auto& segment = segments[segmentIdx];
        segment.segmentData->write(&data, offsetInSegment, dstOffset - segment.offsetInChunk, 1);
    }

    // TODO usage of offsetInChunk might be wrong if we start scanning at non-zero row
    void rangeSegments(common::offset_t offsetInChunk, common::length_t length,
        const range_func_t& func) override {
        common::idx_t segmentIdx = 0;
        auto offsetInSegment = offsetInChunk;
        while (segments[segmentIdx].length < offsetInSegment) {
            KU_ASSERT(segmentIdx < segments.size() - 1);
            offsetInSegment -= segments[segmentIdx].length;
            segmentIdx++;
        }
        uint64_t lengthScanned = 0;
        while (lengthScanned < length) {
            auto lengthInSegment =
                std::min(length - lengthScanned, segments[segmentIdx].length - offsetInSegment);
            func(segmentIdx, offsetInSegment, lengthInSegment, lengthScanned);
            lengthScanned += lengthInSegment;
            segmentIdx++;
            offsetInSegment = 0;
        }
    }

    uint64_t getNumValues() override { return numValues; }

    void initScannedSegmentIfNeeded(common::idx_t segmentIdx) {
        auto& currentSegment = segments[segmentIdx];
        if (currentSegment.segmentData == nullptr) {
            currentSegment.segmentData =
                ColumnChunkFactory::createColumnChunkData(mm, columnType.copy(), enableCompression,
                    currentSegment.length, ResidencyState::IN_MEMORY);

            currentSegment.scanFunc(*currentSegment.segmentData);
        }
    }

    std::vector<SegmentData> segments;

    uint64_t numValues;

    MemoryManager& mm;
    common::LogicalType columnType;
    bool enableCompression;
};
} // namespace storage
} // namespace kuzu
