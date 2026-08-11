/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.query.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.query.rpc.RelationResolution;
import ai.floedb.floecat.query.rpc.UserObjectsBundleChunk;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Direct tests for the GetUserObjects wire-protocol framer. */
class BundleStreamFramerTest {

  private static final int MAX = 25;

  private static List<RelationResolution> resolutions(int from, int count) {
    List<RelationResolution> out = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      out.add(RelationResolution.newBuilder().setInputIndex(from + i).build());
    }
    return out;
  }

  @Test
  void rejectsNonPositiveResolutionChunkLimits() {
    assertThatThrownBy(() -> new BundleStreamFramer("q-1", 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("maxResolutionsPerChunk must be positive");
    assertThatThrownBy(() -> new BundleStreamFramer("q-1", -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("maxResolutionsPerChunk must be positive");
  }

  @Test
  void headerIsFirstAndCarriesSeqOne() {
    BundleStreamFramer framer = new BundleStreamFramer("q-1", MAX);
    assertThat(framer.headerPending()).isTrue();
    assertThat(framer.isOpen()).isTrue();

    UserObjectsBundleChunk header = framer.header();
    assertThat(header.hasHeader()).isTrue();
    assertThat(header.getQueryId()).isEqualTo("q-1");
    assertThat(header.getSeq()).isEqualTo(1);
    assertThat(framer.headerPending()).isFalse();
  }

  @Test
  void slicesBufferedResolutionsIntoMaxSizedChunksInOrderWithMonotonicSeq() {
    BundleStreamFramer framer = new BundleStreamFramer("q-1", MAX);
    framer.header(); // seq 1
    framer.offer(resolutions(0, 60));

    List<Integer> chunkSizes = new ArrayList<>();
    List<Long> seqs = new ArrayList<>();
    int expectedIndex = 0;
    while (framer.hasBufferedResolutions()) {
      UserObjectsBundleChunk c = framer.nextResolutionChunk();
      assertThat(c.hasResolutions()).isTrue();
      chunkSizes.add(c.getResolutions().getItemsCount());
      seqs.add(c.getSeq());
      for (RelationResolution r : c.getResolutions().getItemsList()) {
        assertThat(r.getInputIndex()).isEqualTo(expectedIndex++); // order preserved
      }
    }
    assertThat(chunkSizes).containsExactly(25, 25, 10);
    assertThat(seqs).containsExactly(2L, 3L, 4L); // header was seq 1
    assertThat(expectedIndex).isEqualTo(60);
  }

  @Test
  void buffersRemainderAcrossOffers() {
    BundleStreamFramer framer = new BundleStreamFramer("q-1", MAX);
    framer.header();
    framer.offer(resolutions(0, 10));
    framer.offer(resolutions(10, 20)); // 30 buffered total

    UserObjectsBundleChunk first = framer.nextResolutionChunk();
    assertThat(first.getResolutions().getItemsCount()).isEqualTo(25);
    assertThat(framer.hasBufferedResolutions()).isTrue();

    UserObjectsBundleChunk second = framer.nextResolutionChunk();
    assertThat(second.getResolutions().getItemsCount()).isEqualTo(5);
    assertThat(framer.hasBufferedResolutions()).isFalse();
  }

  @Test
  void endIsEmittedOnceWithCountsAndClosesTheStream() {
    BundleStreamFramer framer = new BundleStreamFramer("q-1", MAX);
    framer.header();
    framer.offer(resolutions(0, 3));
    framer.nextResolutionChunk(); // seq 2

    UserObjectsBundleChunk end = framer.end(3, 2, 1);
    assertThat(end.hasEnd()).isTrue();
    assertThat(end.getSeq()).isEqualTo(3);
    assertThat(end.getEnd().getResolutionCount()).isEqualTo(3);
    assertThat(end.getEnd().getFoundCount()).isEqualTo(2);
    assertThat(end.getEnd().getNotFoundCount()).isEqualTo(1);
    assertThat(framer.isOpen()).isFalse();
  }

  @Test
  void emptyStreamIsHeaderThenEndOnly() {
    BundleStreamFramer framer = new BundleStreamFramer("q-1", MAX);
    UserObjectsBundleChunk header = framer.header();
    assertThat(header.hasHeader()).isTrue();
    assertThat(framer.hasBufferedResolutions()).isFalse();

    UserObjectsBundleChunk end = framer.end(0, 0, 0);
    assertThat(end.hasEnd()).isTrue();
    assertThat(end.getSeq()).isEqualTo(2);
    assertThat(end.getEnd().getResolutionCount()).isZero();
    assertThat(framer.isOpen()).isFalse();
  }
}
