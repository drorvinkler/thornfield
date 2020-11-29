from dataclasses import dataclass
from typing import List, Dict, Any, Optional

from thornfield.caches.cache import Cache
from thornfield.typing import NormalCallable


@dataclass
class CachingData:
    func_args: List[str]
    func_annotations: Dict[str, Any]
    func_defaults: Dict[str, Any]
    cache: Optional[Cache]
    func_passed_to_cache: Optional[NormalCallable]
