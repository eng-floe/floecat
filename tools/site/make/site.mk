# Copyright 2026 Yellowbrick Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Site/docs targets intentionally live outside the root Makefile so
# documentation and website automation can evolve independently from
# core runtime/build targets.

.PHONY: test-site test-site-jekyll test-site-docs test-site-e2e lint-markdown site-preview

test-site: # website sanity build (Jekyll + docs output checks)
	@./tools/site/test-site.sh

test-site-jekyll: # Jekyll-only build + website output checks
	@./tools/site/test-site-jekyll.sh

test-site-docs: # docs-only build + documentation output checks
	@./tools/site/test-site-docs.sh

test-site-e2e: test-site # Playwright smoke checks for rendered site pages
	@./tools/site/test-site-e2e.sh

lint-markdown: # markdown lint for docs/ and site-src markdown
	@./tools/site/lint-markdown.sh

site-preview: # local live preview for /floecat and /floecat/documentation
	@./tools/site/serve-site.sh
