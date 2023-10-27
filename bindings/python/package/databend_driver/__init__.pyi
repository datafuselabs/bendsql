# Copyright 2021 Datafuse Labs
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

# flake8: noqa
class Row:
    def values(self) -> tuple: ...

class RowIterator:
    def __aiter__(self) -> RowIterator: ...
    async def __anext__(self) -> Row: ...

# flake8: noqa
class AsyncDatabendConnection:
    async def exec(self, sql: str) -> int: ...
    async def query_row(self, sql: str) -> Row: ...
    async def query_iter(self, sql: str) -> RowIterator: ...

# flake8: noqa
class AsyncDatabendClient:
    def __init__(self, dsn: str): ...
    async def get_conn(self) -> AsyncDatabendConnection: ...