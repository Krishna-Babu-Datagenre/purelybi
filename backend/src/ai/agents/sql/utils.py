from typing import Any, Dict, List


def unpack_agent_updates(agent_update_list: List[Dict[str, Any]]) -> List[Any]:
    """
    Flatten all messages from a langchain agent.invoke output (stream_mode='updates').

    Args:
        agent_response (list[dict]):
            Output from agent.invoke with stream_mode='updates'.
            Each item is a dict with a single key ('model' or 'tools'), whose value is a dict containing a 'messages' list.

    Returns:
        list: Flat list of all message objects from all 'messages' lists in the input.

    Example:
        >>> output = [
        ...     {'model': {'messages': [AIMessage(...)]}},
        ...     {'tools': {'messages': [ToolMessage(...)]}},
        ... ]
        >>> unpack_agent_updates(output)
        >>> [AIMessage(...), ToolMessage(...)]
    """
    return [m for d in agent_update_list for key in d for m in d[key]["messages"]]
