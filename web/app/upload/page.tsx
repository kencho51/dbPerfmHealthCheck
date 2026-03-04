"use client";

import { useState, useRef } from "react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Spinner } from "@/components/ui/spinner";
import { api, type ValidationResult, type UploadResult } from "@/lib/api";
import { CheckCircle, XCircle, Upload, AlertTriangle } from "lucide-react";

type Step = "idle" | "validating" | "previewing" | "uploading" | "done";

export default function UploadPage() {
  const [step, setStep] = useState<Step>("idle");
  const [file, setFile] = useState<File | null>(null);
  const [validation, setValidation] = useState<ValidationResult | null>(null);
  const [result, setResult] = useState<UploadResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  function reset() {
    setStep("idle");
    setFile(null);
    setValidation(null);
    setResult(null);
    setError(null);
  }

  async function handleFileChange(e: React.ChangeEvent<HTMLInputElement>) {
    const f = e.target.files?.[0];
    if (!f) return;
    setFile(f);
    setStep("validating");
    setError(null);
    try {
      const v = await api.validate(f);
      setValidation(v);
      setStep("previewing");
    } catch (err) {
      setError(String(err));
      setStep("idle");
    }
  }

  async function handleUpload() {
    if (!file) return;
    setStep("uploading");
    setError(null);
    try {
      const r = await api.upload(file);
      setResult(r);
      setStep("done");
    } catch (err) {
      setError(String(err));
      setStep("previewing");
    }
  }

  return (
    <div className="max-w-2xl space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-900">Upload CSV</h1>
        <p className="text-sm text-slate-500 mt-1">
          Two-step: validate preview → confirm ingest
        </p>
      </div>

      {/* Step 1: Drop zone */}
      {(step === "idle" || step === "validating") && (
        <Card>
          <CardContent className="pt-5">
            <div
              className="flex flex-col items-center justify-center gap-3 rounded-lg border-2 border-dashed border-slate-300 p-12 text-center cursor-pointer hover:border-indigo-400 hover:bg-indigo-50/30 transition-colors"
              onClick={() => inputRef.current?.click()}
            >
              {step === "validating" ? (
                <Spinner />
              ) : (
                <Upload className="h-8 w-8 text-slate-400" />
              )}
              <p className="text-sm text-slate-500">
                {step === "validating"
                  ? "Validating…"
                  : "Click or drag a Splunk CSV file here"}
              </p>
              <input
                ref={inputRef}
                type="file"
                accept=".csv"
                className="hidden"
                onChange={handleFileChange}
              />
            </div>
            {error && <p className="mt-3 text-sm text-red-600">{error}</p>}
          </CardContent>
        </Card>
      )}

      {/* Step 2: Validation preview */}
      {step === "previewing" && validation && (
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle>Validation Preview</CardTitle>
              <Badge variant={validation.is_valid ? "success" : "error"}>
                {validation.is_valid ? "✓ Valid" : "✗ Errors"}
              </Badge>
            </div>
            <p className="text-xs text-slate-500 font-mono">{file?.name}</p>
          </CardHeader>
          <CardContent>
            <dl className="grid grid-cols-2 gap-x-6 gap-y-2 text-sm mb-4">
              <div>
                <dt className="text-slate-400 text-xs">Type</dt>
                <dd className="font-medium">{validation.file_type}</dd>
              </div>
              <div>
                <dt className="text-slate-400 text-xs">Environment</dt>
                <dd>
                  <Badge variant={validation.environment === "prod" ? "prod" : "sat"}>
                    {validation.environment}
                  </Badge>
                </dd>
              </div>
              <div>
                <dt className="text-slate-400 text-xs">Rows</dt>
                <dd className="font-medium">{validation.row_count.toLocaleString()}</dd>
              </div>
            </dl>

            {validation.warnings.length > 0 && (
              <div className="mb-3">
                <p className="text-xs font-medium text-amber-600 flex items-center gap-1 mb-1">
                  <AlertTriangle className="h-3 w-3" /> Warnings
                </p>
                <ul className="space-y-1">
                  {validation.warnings.map((w, i) => (
                    <li key={i} className="text-xs text-amber-700 bg-amber-50 rounded px-2 py-1">{w}</li>
                  ))}
                </ul>
              </div>
            )}

            {validation.errors.length > 0 && (
              <div className="mb-3">
                <p className="text-xs font-medium text-red-600 flex items-center gap-1 mb-1">
                  <XCircle className="h-3 w-3" /> Errors
                </p>
                <ul className="space-y-1">
                  {validation.errors.map((e, i) => (
                    <li key={i} className="text-xs text-red-700 bg-red-50 rounded px-2 py-1">{e}</li>
                  ))}
                </ul>
              </div>
            )}

            {/* Null rates */}
            {Object.keys(validation.null_rates).length > 0 && (
              <div className="mb-4">
                <p className="text-xs font-medium text-slate-500 mb-1">Null rates (required columns)</p>
                <div className="grid grid-cols-2 gap-1">
                  {Object.entries(validation.null_rates).map(([col, rate]) => (
                    <div key={col} className="flex justify-between text-xs">
                      <span className="text-slate-500">{col}</span>
                      <span className={rate > 0.1 ? "text-red-600" : "text-slate-700"}>
                        {(rate * 100).toFixed(1)}%
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            <div className="flex gap-3">
              <Button
                onClick={handleUpload}
                disabled={!validation.is_valid}
                className="flex-1"
              >
                Confirm & Ingest
              </Button>
              <Button variant="outline" onClick={reset}>
                Cancel
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Uploading */}
      {step === "uploading" && (
        <Card>
          <CardContent className="pt-5 flex items-center gap-3">
            <Spinner />
            <span className="text-sm text-slate-600">Ingesting…</span>
          </CardContent>
        </Card>
      )}

      {/* Done */}
      {step === "done" && result && (
        <Card>
          <CardHeader>
            <div className="flex items-center gap-2">
              <CheckCircle className="h-5 w-5 text-green-500" />
              <CardTitle className="text-green-700">Ingest complete</CardTitle>
            </div>
            <p className="text-xs text-slate-500 font-mono">{result.filename}</p>
          </CardHeader>
          <CardContent>
            <dl className="grid grid-cols-3 gap-4 text-center text-sm mb-4">
              <div>
                <dt className="text-slate-400 text-xs">Inserted</dt>
                <dd className="text-2xl font-bold text-indigo-600">{result.inserted.toLocaleString()}</dd>
              </div>
              <div>
                <dt className="text-slate-400 text-xs">Updated</dt>
                <dd className="text-2xl font-bold text-teal-600">{result.updated.toLocaleString()}</dd>
              </div>
              <div>
                <dt className="text-slate-400 text-xs">Skipped</dt>
                <dd className="text-2xl font-bold text-slate-400">{result.skipped.toLocaleString()}</dd>
              </div>
            </dl>
            <Button variant="outline" onClick={reset} className="w-full">
              Upload another file
            </Button>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
