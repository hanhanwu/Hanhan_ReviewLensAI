import React, { ChangeEvent, CSSProperties, useEffect, useMemo, useState } from "react";
import { StatusBar } from "expo-status-bar";
import {
  NativeSyntheticEvent,
  Platform,
  Pressable,
  SafeAreaView,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  TextInputKeyPressEventData,
  View,
} from "react-native";

const BACKEND_URL = "http://127.0.0.1:8000";

type UploadResponse = {
  upload_id: string;
  rows: number;
  columns: number;
  filename?: string;
};

type BackendStats = {
  rows: number;
  columns: number;
  column_names: string[];
  missing_by_column: Record<string, number>;
  rating_counts: Array<{ rating: string; count: number }>;
  rating_category_counts: Array<{ category: string; count: number }>;
};

type DbAggregates = {
  upload_id: string;
  filename: string;
  created_at: string | null;
  rows_count: number;
  columns_count: number;
  column_names: string[];
  column_value_counts: Array<{
    column: string;
    present_count: number;
    non_null_count: number;
  }>;
  rating_category_counts: Array<{ category: string; count: number }>;
  rating_counts: Array<{ rating: string; count: number }>;
};

type ChatMessage = {
  role: "user" | "assistant";
  content: string;
};

type ChatResponse = {
  answer: string;
  model: string | null;
};

const fileInputStyle: CSSProperties = {
  padding: 12,
  borderRadius: 8,
  border: "1px solid #8a8a8a",
  fontSize: 16,
  width: "100%",
  maxWidth: 360,
  marginTop: 16,
  cursor: "pointer",
  backgroundColor: "#fff",
};

export default function App() {
  const [upload, setUpload] = useState<UploadResponse | null>(null);
  const [backendStats, setBackendStats] = useState<BackendStats | null>(null);
  const [dbAggregates, setDbAggregates] = useState<DbAggregates | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadingBackend, setLoadingBackend] = useState(false);
  const [loadingDb, setLoadingDb] = useState(false);
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>([]);
  const [chatInput, setChatInput] = useState("");
  const [chatLoading, setChatLoading] = useState(false);
  const [chatScrollRef, setChatScrollRef] = useState<ScrollView | null>(null);

  const handleFileChange = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }

    setUpload(null);
    setBackendStats(null);
    setDbAggregates(null);
    setChatMessages([]);
    setChatInput("");
    setLoading(true);
    setError(null);

    try {
      const formData = new FormData();
      formData.append("file", file);

      const response = await fetch(`${BACKEND_URL}/upload`, {
        method: "POST",
        body: formData,
      });

      if (!response.ok) {
        const payload = await response.json().catch(() => null);
        const detail =
          payload && typeof payload.detail === "string"
            ? payload.detail
            : "Unable to upload the file.";
        throw new Error(detail);
      }

      const payload: UploadResponse = await response.json();
      setUpload(payload);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Upload failed.");
    } finally {
      setLoading(false);
      event.target.value = "";
    }
  };

  useEffect(() => {
    if (!upload?.upload_id) {
      return;
    }

    let cancelled = false;
    setLoadingBackend(true);
    setLoadingDb(true);
    setBackendStats(null);
    setDbAggregates(null);
    setError(null);

    fetch(`${BACKEND_URL}/uploads/${upload.upload_id}/backend-stats`)
      .then(async (r) => {
        if (!r.ok) {
          const payload = await r.json().catch(() => null);
          const detail =
            payload && typeof payload.detail === "string"
              ? payload.detail
              : "Failed to load backend stats.";
          throw new Error(detail);
        }
        return (await r.json()) as BackendStats;
      })
      .then((left) => {
        if (cancelled) return;
        setBackendStats(left);
      })
      .catch((err) => {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Failed to load backend stats.");
      })
      .finally(() => {
        if (cancelled) return;
        setLoadingBackend(false);
      });

    fetch(`${BACKEND_URL}/uploads/${upload.upload_id}/db-aggregates`)
      .then(async (r) => {
        if (!r.ok) {
          const payload = await r.json().catch(() => null);
          const detail =
            payload && typeof payload.detail === "string"
              ? payload.detail
              : "Failed to load database aggregates.";
          throw new Error(detail);
        }
        return (await r.json()) as DbAggregates;
      })
      .then((right) => {
        if (cancelled) return;
        setDbAggregates(right);
      })
      .catch((err) => {
        if (cancelled) return;
        setError(
          err instanceof Error ? err.message : "Failed to load database aggregates."
        );
      })
      .finally(() => {
        if (cancelled) return;
        setLoadingDb(false);
      });

    return () => {
      cancelled = true;
    };
  }, [upload?.upload_id]);

  useEffect(() => {
    chatScrollRef?.scrollToEnd({ animated: true });
  }, [chatMessages, chatLoading, chatScrollRef]);

  const missingList = useMemo(() => {
    if (!backendStats?.missing_by_column) return [];
    return Object.entries(backendStats.missing_by_column)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 12);
  }, [backendStats?.missing_by_column]);

  const hasLeftData = !!backendStats;
  const titleStyle = hasLeftData ? [styles.title, styles.titleLeft] : styles.title;

  const sendChatMessage = async () => {
    const question = chatInput.trim();
    if (!upload?.upload_id || !question || chatLoading) {
      return;
    }

    const nextHistory = [...chatMessages, { role: "user" as const, content: question }];
    setChatMessages(nextHistory);
    setChatInput("");
    setChatLoading(true);
    setError(null);

    try {
      const response = await fetch(`${BACKEND_URL}/uploads/${upload.upload_id}/chat`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          question,
          history: chatMessages,
        }),
      });

      if (!response.ok) {
        const payload = await response.json().catch(() => null);
        const detail =
          payload && typeof payload.detail === "string"
            ? payload.detail
            : "Failed to get chatbot response.";
        throw new Error(detail);
      }

      const payload: ChatResponse = await response.json();
      setChatMessages((current) => [
        ...current,
        { role: "assistant", content: payload.answer || "I don't know" },
      ]);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Chat request failed.");
      setChatMessages((current) => [
        ...current,
        { role: "assistant", content: "I don't know" },
      ]);
    } finally {
      setChatLoading(false);
    }
  };

  const handleChatKeyPress = (
    event: NativeSyntheticEvent<TextInputKeyPressEventData>
  ) => {
    if (event.nativeEvent.key === "Enter") {
      void sendChatMessage();
    }
  };

  return (
    <SafeAreaView style={styles.screen}>
      <StatusBar style="auto" />
      <ScrollView
        style={styles.scroll}
        contentContainerStyle={styles.scrollContent}
        keyboardShouldPersistTaps="handled"
      >
        <Text style={titleStyle}>ReviewLens</Text>

        {Platform.OS === "web" && !hasLeftData ? (
          <View style={styles.uploadCenterWrap}>
            <input
              type="file"
              accept=".csv,text/csv"
              onChange={handleFileChange}
              disabled={loading}
              style={fileInputStyle}
            />
          </View>
        ) : Platform.OS !== "web" ? (
          <Text style={styles.note}>
            File upload is available in the web build only. Please open the app in a
            browser.
          </Text>
        ) : null}

        {error && <Text style={styles.error}>{error}</Text>}

        {upload && (
          <View style={styles.dashboard}>
            <View style={styles.leftColumn}>
              <View style={styles.panel}>
                <Text style={styles.panelTitle}>Backend Aggregates</Text>
                {loadingBackend && <Text style={styles.panelHint}>Loading...</Text>}
                {backendStats && (
                  <View>
                    <Text style={styles.kvLabel}>Rows</Text>
                    <Text style={styles.kvValue}>{backendStats.rows}</Text>
                    <Text style={styles.kvLabel}>Columns</Text>
                    <Text style={styles.kvValue}>{backendStats.columns}</Text>

                    <Text style={styles.sectionTitle}>Top Missing Columns</Text>
                    {missingList.length === 0 ? (
                      <Text style={styles.panelHint}>No missing-value stats.</Text>
                    ) : (
                      missingList.map(([col, count]) => (
                        <Text key={col} style={styles.listItem}>
                          {col}: {count}
                        </Text>
                      ))
                    )}

                    {backendStats.rating_category_counts?.length > 0 && (
                      <>
                        <Text style={styles.sectionTitle}>Rating Categories</Text>
                        {backendStats.rating_category_counts.slice(0, 8).map((r) => (
                          <Text key={r.category} style={styles.listItem}>
                            {r.category}: {r.count}
                          </Text>
                        ))}
                      </>
                    )}

                    {backendStats.rating_counts?.length > 0 && (
                      <>
                        <Text style={styles.sectionTitle}>Ratings</Text>
                        {backendStats.rating_counts.slice(0, 8).map((r) => (
                          <Text key={r.rating} style={styles.listItem}>
                            {r.rating}: {r.count}
                          </Text>
                        ))}
                      </>
                    )}
                  </View>
                )}
              </View>

              {Platform.OS === "web" && hasLeftData && (
                <View style={styles.uploadBelowLeft}>
                  <input
                    type="file"
                    accept=".csv,text/csv"
                    onChange={handleFileChange}
                    disabled={loading}
                    style={fileInputStyle}
                  />
                </View>
              )}
            </View>

            <View style={styles.panel}>
              <Text style={styles.panelTitle}>Data Chatbot</Text>
              <Text style={styles.panelHint}>
                Ask about the uploaded dataset stored in the database.
              </Text>
              {loadingDb && <Text style={styles.panelHint}>Loading data context...</Text>}
              <ScrollView
                ref={setChatScrollRef}
                style={styles.chatMessages}
                contentContainerStyle={styles.chatMessagesContent}
                onContentSizeChange={() =>
                  chatScrollRef?.scrollToEnd({ animated: true })
                }
                keyboardShouldPersistTaps="handled"
              >
                {chatMessages.length === 0 ? (
                  <Text style={styles.chatEmpty}>
                    Try questions like "How many rows are in the data?" or "What ratings
                    appear most often?"
                  </Text>
                ) : (
                  chatMessages.map((message, index) => (
                    <View
                      key={`${message.role}-${index}`}
                      style={[
                        styles.chatBubble,
                        message.role === "user"
                          ? styles.chatBubbleUser
                          : styles.chatBubbleAssistant,
                      ]}
                    >
                      <Text style={styles.chatRole}>
                        {message.role === "user" ? "You" : "ReviewLens Bot"}
                      </Text>
                      <Text style={styles.chatText}>{message.content}</Text>
                    </View>
                  ))
                )}
                {chatLoading && <Text style={styles.panelHint}>Thinking...</Text>}
              </ScrollView>
              <View style={styles.chatInputRow}>
                <TextInput
                  value={chatInput}
                  onChangeText={setChatInput}
                  placeholder="Ask about this dataset"
                  placeholderTextColor="#7a7a7a"
                  style={styles.chatInput}
                  editable={!chatLoading && !!upload?.upload_id}
                  multiline
                  blurOnSubmit={false}
                  onKeyPress={handleChatKeyPress}
                />
                <Pressable
                  style={[styles.chatSendButton, chatLoading && styles.chatSendButtonDisabled]}
                  onPress={sendChatMessage}
                  disabled={chatLoading || !upload?.upload_id}
                >
                  <Text style={styles.chatSendText}>Send</Text>
                </Pressable>
              </View>
            </View>
          </View>
        )}
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  screen: {
    flex: 1,
    backgroundColor: "#f2f2f7",
  },
  scroll: {
    flex: 1,
  },
  scrollContent: {
    paddingHorizontal: 16,
    paddingTop: 16,
    paddingBottom: 48,
    alignItems: "stretch",
  },
  title: {
    fontSize: 28,
    fontWeight: "700",
    marginTop: 12,
    textAlign: "center",
    alignSelf: "center",
  },
  titleLeft: {
    textAlign: "left",
    alignSelf: "flex-start",
    marginTop: 4,
  },
  note: {
    marginTop: 16,
    color: "#555",
    textAlign: "center",
    maxWidth: 420,
  },
  error: {
    marginTop: 8,
    color: "#d93025",
  },
  uploadCenterWrap: {
    width: "100%",
    alignItems: "center",
    justifyContent: "center",
    marginTop: 8,
  },
  dashboard: {
    marginTop: 16,
    width: "100%",
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 16,
    alignItems: "flex-start",
    justifyContent: "space-between",
  },
  leftColumn: {
    flex: 1,
    minWidth: 280,
  },
  panel: {
    flex: 1,
    minWidth: 280,
    backgroundColor: "#fff",
    borderRadius: 16,
    padding: 18,
    shadowColor: "#000",
    shadowOffset: { width: 0, height: 6 },
    shadowOpacity: 0.08,
    shadowRadius: 12,
    elevation: 6,
  },
  panelTitle: {
    fontSize: 16,
    fontWeight: "700",
    color: "#111",
    marginBottom: 8,
  },
  panelHint: {
    fontSize: 13,
    color: "#6b7280",
    marginBottom: 8,
  },
  kvLabel: {
    fontSize: 11,
    color: "#6b7280",
    marginTop: 10,
    textTransform: "uppercase",
  },
  kvValue: {
    fontSize: 20,
    fontWeight: "700",
    color: "#111",
  },
  sectionTitle: {
    marginTop: 14,
    fontSize: 12,
    fontWeight: "700",
    color: "#111827",
    textTransform: "uppercase",
  },
  listItem: {
    marginTop: 6,
    fontSize: 14,
    color: "#111",
  },
  uploadBelowLeft: {
    marginTop: 18,
    alignItems: "flex-start",
  },
  chatMessages: {
    marginTop: 8,
    minHeight: 320,
    maxHeight: 420,
  },
  chatMessagesContent: {
    gap: 10,
  },
  chatEmpty: {
    color: "#555",
    fontSize: 14,
    lineHeight: 20,
  },
  chatBubble: {
    borderRadius: 14,
    padding: 12,
  },
  chatBubbleUser: {
    backgroundColor: "#e7f0ff",
    alignSelf: "flex-end",
    maxWidth: "88%",
  },
  chatBubbleAssistant: {
    backgroundColor: "#f4f4f4",
    alignSelf: "flex-start",
    maxWidth: "92%",
  },
  chatRole: {
    fontSize: 11,
    fontWeight: "700",
    color: "#475569",
    marginBottom: 4,
    textTransform: "uppercase",
  },
  chatText: {
    fontSize: 14,
    lineHeight: 20,
    color: "#111",
  },
  chatInputRow: {
    marginTop: 14,
    flexDirection: "row",
    alignItems: "flex-end",
    gap: 10,
  },
  chatInput: {
    flex: 1,
    minHeight: 88,
    borderWidth: 1,
    borderColor: "#cbd5e1",
    borderRadius: 12,
    paddingHorizontal: 12,
    paddingVertical: 12,
    backgroundColor: "#fff",
    textAlignVertical: "top",
  },
  chatSendButton: {
    alignSelf: "flex-end",
    backgroundColor: "#1f4fd1",
    paddingHorizontal: 18,
    paddingVertical: 10,
    borderRadius: 10,
  },
  chatSendButtonDisabled: {
    opacity: 0.6,
  },
  chatSendText: {
    color: "#fff",
    fontWeight: "700",
  },
});
