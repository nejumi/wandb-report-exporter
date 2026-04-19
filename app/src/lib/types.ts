export interface ManifestPage {
  slug: string;
  title: string;
  description: string;
  datasets: string[];
  widgets: string[];
}

export interface ReportManifest {
  generated_at: string;
  source: string;
  entity?: string | null;
  project?: string | null;
  report_url?: string | null;
  counts: {
    runs: number;
    history_rows: number;
    table_rows: number;
    media_items: number;
  };
  datasets: Record<string, string>;
  pages: ManifestPage[];
}

export interface SummaryRow {
  run_id: string;
  run_name: string;
  model_name: string;
  dataset_name: string;
  eval_split: string;
  state: string;
  accuracy?: number;
  loss?: number;
  primary_metric?: number;
  wandb_url?: string;
}

export interface HistoryRow {
  run_id: string;
  step: number;
  epoch?: number;
  metric_name: string;
  metric_value: number;
  timestamp?: string;
}

export interface FailureCaseRow {
  row_id: string;
  run_id: string;
  run_name?: string;
  model_name?: string;
  dataset_name?: string;
  split?: string;
  slice_name?: string;
  input_text?: string;
  prediction?: string;
  label?: string;
  correct?: boolean;
  score?: number;
  image_thumb_path?: string;
  image_full_path?: string;
  wandb_run_url?: string;
  wandb_artifact_url?: string;
  meta_json?: string;
}
