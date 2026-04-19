import process from "node:process";
import * as vega from "vega";
import * as vegaLite from "vega-lite";

let stdin = "";
for await (const chunk of process.stdin) {
  stdin += chunk;
}
const input = JSON.parse(stdin);
const sourceSpec = input?.spec;
let targetWidth = Number(input?.width) > 0 ? Number(input.width) : 720;
let targetHeight = Number(input?.height) > 0 ? Number(input.height) : 480;

if (!sourceSpec || typeof sourceSpec !== "object" || Array.isArray(sourceSpec)) {
  throw new Error("Expected a Vega or Vega-Lite spec object.");
}

function isRadialLikeSpec(node) {
  if (!node || typeof node !== "object") {
    return false;
  }
  const serialized = JSON.stringify(node);
  return serialized.includes("cos(scale(")
    && serialized.includes("sin(scale(")
    && serialized.includes("\"radius\"")
    && (serialized.includes("\"angular\"") || serialized.includes("\"radial\""));
}

if (isRadialLikeSpec(sourceSpec)) {
  const squareSide = Math.max(targetWidth, targetHeight, 720);
  targetWidth = squareSide;
  targetHeight = squareSide;
}

function normalizeContainerSizing(node) {
  if (Array.isArray(node)) {
    return node.map(normalizeContainerSizing);
  }
  if (!node || typeof node !== "object") {
    return node;
  }
  const normalized = {...node};
  for (const [key, value] of Object.entries(normalized)) {
    if (value === "container") {
      normalized[key] = key.toLowerCase().includes("height") ? targetHeight : targetWidth;
      continue;
    }
    normalized[key] = normalizeContainerSizing(value);
  }
  if (Array.isArray(normalized.signals)) {
    normalized.signals = normalized.signals.map((signal) => {
      if (!signal || typeof signal !== "object") {
        return signal;
      }
      if (signal.name === "width" && typeof signal.update === "string" && signal.update.includes("containerSize()[0]")) {
        return {name: signal.name, value: targetWidth};
      }
      if (signal.name === "height" && typeof signal.update === "string" && signal.update.includes("containerSize()[1]")) {
        return {name: signal.name, value: targetHeight};
      }
      return signal;
    });
  }
  return normalized;
}

const normalizedSourceSpec = normalizeContainerSizing(sourceSpec);

const schema = String(normalizedSourceSpec.$schema || "").toLowerCase();
const compiled =
  schema.includes("vega-lite") || normalizedSourceSpec.encoding || normalizedSourceSpec.layer || normalizedSourceSpec.facet || normalizedSourceSpec.repeat
    ? vegaLite.compile(normalizedSourceSpec).spec
    : normalizedSourceSpec;

const view = new vega.View(vega.parse(compiled), {
  renderer: "none"
}).width(targetWidth).height(targetHeight);

const svg = await view.toSVG();
process.stdout.write(svg);
