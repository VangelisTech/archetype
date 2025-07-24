from dataclasses import dataclass

class ArchetypeConfig: 
    uri: str
    namespace: str
    debug: bool
    #auth_enabled: bool
    #token_limit_bidget:int

    @classmethod
    def from_env(cls) -> 'ArchetypeConfig':
        pass
    
    @classmethod
    def from_yaml(cls) -> 'ArchetypeConfig':
        pass

