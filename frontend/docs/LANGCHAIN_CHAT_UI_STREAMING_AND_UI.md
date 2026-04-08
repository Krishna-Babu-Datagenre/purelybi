# LangChain Agent Chat UI – Streaming, Tool Calls & Thought Process

Summary of how the [LangChain Agent Chat UI](https://github.com/langchain-ai/agent-chat-ui) (at `agent-chat-ui-main`) handles streaming, tool calls, and thought/state UI. Paths below are relative to that repo root.

---

## 1. Streaming from the backend

**Mechanism:** The app does **not** implement fetch/SSE/WebSocket itself. All streaming is handled by **`@langchain/langgraph-sdk`** via the React hook **`useStream`**.

- **Provider:** `src/providers/Stream.tsx`
  - Typed wrapper: `useTypedStream = useStream<StateType, UpdateType>` (lines 30–40).
  - `StreamSession` calls `useTypedStream({ apiUrl, apiKey, assistantId, threadId, fetchStateHistory: true, onCustomEvent, onThreadId })`.
  - Stream context is provided to the tree via `StreamContext.Provider`; components use `useStreamContext()`.

- **Submit options (streaming config):** `src/components/thread/index.tsx`
  - On user send (lines 217–230) and regenerate (245–249):
    - `stream.submit(payload, { streamMode: ["values"], streamSubgraphs: true, streamResumable: true, ... })`.
  - **Parsing of chunks:** Not done in this repo; the SDK consumes the LangGraph API and updates `stream.messages` / `stream.values` as streamed **values** (state snapshots) arrive.

- **API proxy:** `src/app/api/[..._path]/route.ts`
  - Uses `langgraph-nextjs-api-passthrough` to proxy requests to the LangGraph server (no custom streaming logic here).

**Takeaway:** Streaming is **value-based** (state updates), not raw token/SSE parsing in the frontend. The SDK owns the transport and parsing; the UI just reads `stream.messages` and re-renders.

---

## 2. Tool calls / agent steps in the UI

**State:** Tool calls live on **messages** from `stream.messages`. AI messages can have `message.tool_calls`; tool results are separate `type: "tool"` messages.

- **Ensuring tool responses:** `src/lib/ensure-tool-responses.ts`
  - `ensureToolCallsHaveResponses(messages)`: for each AI message with `tool_calls`, if the next message is not a tool message, it pushes placeholder tool messages (`content: "Successfully handled tool call."`) so the graph can resume. Used before `stream.submit()` in `src/components/thread/index.tsx` (line 212).

- **Rendering tool calls (AI message):** `src/components/thread/messages/ai.tsx`
  - `AssistantMessage` reads `message.content` and `message.tool_calls`.
  - Supports both standard `tool_calls` and Anthropic-style streamed content: `parseAnthropicStreamedToolCalls(content)` (lines 46–67) maps `content` items with `type === "tool_use"` to `{ name, id, args, type: "tool_call" }`, using `parsePartialJson` for partial `input`.
  - Renders:
    - Text: `getContentString(content)` → `<MarkdownText>`.
    - Tool calls: `<ToolCalls toolCalls={...} />` (or streamed Anthropic variant). Can be hidden via query param `hideToolCalls`.

- **ToolCalls component:** `src/components/thread/messages/tool-calls.tsx`
  - **`ToolCalls`** (lines 10–66): takes `AIMessage["tool_calls"]`, renders each with a bordered card: tool name, optional id, and a table of `args` (key/value). Complex values shown as JSON.

- **Tool results:** Same file, **`ToolResult`** (lines 68–195):
  - Takes a `ToolMessage`; parses `message.content` as JSON when possible.
  - **Expand/collapse:** `useState(false)` for `isExpanded`. Long content (>4 lines or >500 chars) is truncated when collapsed; a button toggles expand. Uses `framer-motion` (`AnimatePresence`, `motion.div` with `key={isExpanded ? "expanded" : "collapsed"}`) for transition.

- **State / agent step view:** `src/components/thread/agent-inbox/components/state-view.tsx`
  - **`StateView`** shows `values` (e.g. graph state). **`StateViewObject`** (lines 176–227): each key gets a chevron; click toggles `expanded` and shows children via **`StateViewRecursive`**.
  - **`MessagesRenderer`** (43–74): for arrays of messages, renders type label, content, and if present `msg.tool_calls` via **`ToolCallTable`** (`src/components/thread/agent-inbox/components/tool-call-table.tsx`).
  - Expand/collapse: `motion.div` with `height: expanded ? "auto" : 0`, `opacity`, and a rotating chevron (`animate={{ rotate: expanded ? 90 : 0 }}`).

---

## 3. Thought process / tool messages (expand–collapse)

- **Tool results (tool messages):** `src/components/thread/messages/tool-calls.tsx` – **`ToolResult`**
  - Collapsed: truncate to 4 lines or 500 chars; show “Show more” style button (ChevronDown/ChevronUp).
  - Expanded: full content; tables for JSON objects/arrays.

- **Generic “interrupt” / state blob:** `src/components/thread/messages/generic-interrupt.tsx` – **`GenericInterruptView`**
  - For interrupt payloads (object or array). `isExpanded` state; when collapsed, truncates long strings (except URLs), limits array to 2 items, and truncates large nested objects.
  - Table of key/value rows; `maxHeight: 500px` when collapsed; button with ChevronDown/ChevronUp to expand.

- **State view (agent step / state tree):** `src/components/thread/agent-inbox/components/state-view.tsx`
  - **`StateViewObject`:** per-key expand/collapse with chevron; content in **`StateViewRecursive`** (messages, arrays, objects rendered recursively).

---

## 4. Streaming the final assistant message

- **Source of truth:** The last assistant message is the last `type === "ai"` entry in `stream.messages`. The SDK updates this message (and the array) as the stream progresses.

- **Rendering:** `src/components/thread/messages/ai.tsx` – **`AssistantMessage`**
  - `contentString = getContentString(message?.content ?? [])` then `<MarkdownText>{contentString}</MarkdownText>` (lines 109–110, 160–162).
  - **`getContentString`:** `src/components/thread/utils.ts` – if `content` is a string, returns it; else collects all `type === "text"` parts and joins with space. So the assistant text is effectively **chunk-by-chunk** (or segment-by-segment) as the SDK pushes new state with updated `content`, not character-by-character in the UI (the SDK may still receive tokens; the UI just sees updated message content).

- **Loading state:** Before the first AI message appears, `firstTokenReceived` is false and **`AssistantMessageLoading`** (pulsing dots) is shown (`src/components/thread/index.tsx` lines 431–434). `firstTokenReceived` is set when `messages.length` changes and the last message is `type === "ai"`.

---

## 5. API types, hooks, utilities

- **Stream context type:** `src/providers/Stream.tsx`
  - `StreamContextType = ReturnType<typeof useTypedStream>`; `useStreamContext()` returns this (submit, stop, messages, values, isLoading, error, interrupt, getMessagesMetadata, setBranch, etc.).

- **State type for stream:** Same file: `StateType = { messages: Message[]; ui?: UIMessage[] }`. `UpdateType` includes `messages`, `ui`, and `context`. Custom events use `UIMessage` / `RemoveUIMessage` from `@langchain/langgraph-sdk/react-ui` with `uiMessageReducer`.

- **Messages and types:** From `@langchain/langgraph-sdk`: `Message`, `AIMessage`, `ToolMessage`, `Checkpoint`. From `@langchain/core`: `MessageContentComplex`, `parsePartialJson` (for streamed tool args).

- **Hooks:**
  - `useStreamContext()` – stream state and actions.
  - `use-interrupted-actions.tsx` – `streaming`, `streamFinished`, etc., for the agent-inbox action flow.

- **Utilities:**
  - **`getContentString`** (`src/components/thread/utils.ts`) – message content to string for display.
  - **`ensureToolCallsHaveResponses`** (`src/lib/ensure-tool-responses.ts`) – pad tool responses before submit.
  - **`parseAnthropicStreamedToolCalls`** (`src/components/thread/messages/ai.tsx`) – content blocks with `type === "tool_use"` → `tool_calls` shape.

---

## File reference

| Topic              | File(s) |
|--------------------|--------|
| Streaming setup    | `src/providers/Stream.tsx` |
| Submit & stream config | `src/components/thread/index.tsx` |
| API proxy          | `src/app/api/[..._path]/route.ts` |
| AI message + tool calls | `src/components/thread/messages/ai.tsx` |
| Tool call/result UI | `src/components/thread/messages/tool-calls.tsx` |
| Tool response padding | `src/lib/ensure-tool-responses.ts` |
| Thought/state expand–collapse | `src/components/thread/agent-inbox/components/state-view.tsx` |
| Generic interrupt blob | `src/components/thread/messages/generic-interrupt.tsx` |
| Message content → string | `src/components/thread/utils.ts` |
| Agent-inbox types  | `src/components/thread/agent-inbox/types.ts` |

Streaming behavior is fully driven by **`@langchain/langgraph-sdk`** (and `react-ui`); this codebase focuses on how to **consume** `messages`/`values` and render tool calls and expandable thought/state blocks.
