from typing import List
from openai.types.chat import (
    # message envelopes  ─────────────────────────────────────────
    ChatCompletionMessageParam,               # root Union of all roles
    ChatCompletionSystemMessageParam,
    ChatCompletionUserMessageParam,
    ChatCompletionAssistantMessageParam,
    ChatCompletionToolMessageParam,
    ChatCompletionFunctionMessageParam,

    # multimodal content blocks  ────────────────────────────────
    ChatCompletionContentPartParam,           # master Union below
    ChatCompletionContentPartTextParam,       # {"type":"text",       "text":...}
    ChatCompletionContentPartImageParam,      # {"type":"image_url",  "image_url": {...}}
    ChatCompletionContentPartImageFileParam,  # {"type":"image_file", "image_file": {...}}
    # 🆕 1.25.x – GPT-4o
    ChatCompletionContentPartInputAudioParam, # {"type":"audio_url",  "audio_url": {...}}
)

from .archetype import Archetype


# Chat.Text Archetype ----------------------
class ChatText(Component):
    role: ChatCompletionMessageParam
    content: ChatCompletionContentPartTextParam

@processor(ChatText)
class ChatTextProcessor(Processor):

class ChatTextArchetype(Archetype):
    components = []
    p



# Chat.Vision Archetype ----------------------
default = {
    "chat":{
        "text": True,
        "vision": True,
        "tools":{
            "choice": True,
            "regex": False,
            "json_shema": True,
            "grammar":False
        }
    },
    "image":{
        "generate": False,
        "edit": False,
        "variation":False,
    },
    "audio":{
        "translate": False,
        "speech": False,
    }
}


class AgentArchetypeFactory:
    def __init__(self, world):
        self.world = world

    def add_archetype(self, text, vision, tools, transcribe, translate, speech, , num_entities):
        for processor in ChatTextArchetype.processors:
            self.world.add_processor(processor)

        for _ in num_entities:
            self.world.add_entity(ChatTextArchetype.components)

        return self.world
