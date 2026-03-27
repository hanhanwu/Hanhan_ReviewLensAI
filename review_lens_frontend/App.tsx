import React, { ChangeEvent, CSSProperties, useState } from "react";
import { Platform, StatusBar, StyleSheet, Text, View } from "react-native";

const BACKEND_URL = "http://127.0.0.1:8000";

type UploadSummary = {
  rows: number;
  columns: number;
  filename?: string;
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
  const [stats, setStats] = useState<UploadSummary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState("Waiting for a CSV upload.");

  const handleFileChange = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }

    setLoading(true);
    setError(null);
    setMessage("Uploading…");

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

      const summary: UploadSummary = await response.json();
      setStats(summary);
      setMessage("Upload successful.");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Upload failed.");
      setMessage("Upload failed.");
    } finally {
      setLoading(false);
      event.target.value = "";
    }
  };

  return (
    <View style={styles.container}>
      <StatusBar style="auto" />
      <Text style={styles.title}>CSV Column & Row Counter</Text>
      <Text style={styles.subtitle}>
        Upload a CSV file and the backend will calculate the number of rows
        and columns for you.
      </Text>

      {Platform.OS === "web" ? (
        <input
          type="file"
          accept=".csv,text/csv"
          onChange={handleFileChange}
          disabled={loading}
          style={fileInputStyle}
        />
      ) : (
        <Text style={styles.note}>
          File upload is available in the web build only. Please open the app in
          a browser.
        </Text>
      )}

      <Text style={styles.message}>{message}</Text>
      {error && <Text style={styles.error}>{error}</Text>}

      {stats && (
        <View style={styles.resultCard}>
          <Text style={styles.resultLabel}>Filename</Text>
          <Text style={styles.resultValue}>
            {stats.filename ?? "Uploaded CSV file"}
          </Text>
          <Text style={styles.resultLabel}>Rows</Text>
          <Text style={styles.resultValue}>{stats.rows}</Text>
          <Text style={styles.resultLabel}>Columns</Text>
          <Text style={styles.resultValue}>{stats.columns}</Text>
        </View>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    padding: 24,
    backgroundColor: "#f2f2f7",
    alignItems: "center",
    justifyContent: "flex-start",
  },
  title: {
    fontSize: 28,
    fontWeight: "700",
    marginTop: 32,
    textAlign: "center",
  },
  subtitle: {
    fontSize: 16,
    color: "#4a4a4a",
    textAlign: "center",
    marginTop: 12,
    maxWidth: 420,
  },
  note: {
    marginTop: 16,
    color: "#555",
    textAlign: "center",
    maxWidth: 420,
  },
  message: {
    marginTop: 12,
    fontSize: 14,
    color: "#333",
  },
  error: {
    marginTop: 8,
    color: "#d93025",
  },
  resultCard: {
    marginTop: 24,
    width: "100%",
    maxWidth: 380,
    backgroundColor: "#fff",
    borderRadius: 16,
    padding: 20,
    shadowColor: "#000",
    shadowOffset: { width: 0, height: 6 },
    shadowOpacity: 0.1,
    shadowRadius: 12,
    elevation: 6,
  },
  resultLabel: {
    fontSize: 12,
    color: "#777",
    marginTop: 10,
    textTransform: "uppercase",
  },
  resultValue: {
    fontSize: 22,
    fontWeight: "600",
    color: "#111",
  },
});
