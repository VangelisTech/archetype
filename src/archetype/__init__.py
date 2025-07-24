# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from .core.interfaces import Component
from .core.processor import Processor
from .core.aio.async_processor import AsyncProcessor
from .core.orchestrator import WorldOrchestrator
from .core.aio.async_broker import AsyncCommandBroker


def __init__(
    uri: str = None,
    namespace: str = None,
    catalog: Catalog = None, 
    io_config: IOConfig = None, 
    debug: bool = False, 
) -> WorldOrchestrator:
        # 
    uri = uri or ".archetype_data"
    namespace = namespace or "archetypes"
    catalog = catalog or Catalog.from_iceberg(
        SqlCatalog(
            "default",
            **{
                "uri": f"sqlite:///{uri}/catalog.db",
                "warehouse": f"file://{uri}",
            },
        )
    )
    io_config = io_config or IOConfig()
    debug = debug

    """
    Initialize the Archetype Orchestrator with the given parameters.
    
    :param uri: The URI for the catalog.
    :param namespace: The namespace for the archetypes.
    :param catalog: Optional catalog instance.
    :param io_config: Optional IO configuration.
    :param debug: Enable debug mode.
    :return: An instance of WorldOrchestrator.
    """
    from .core.orchestrator import WorldOrchestrator
    return WorldOrchestrator(uri, namespace, catalog, io_config, debug)

