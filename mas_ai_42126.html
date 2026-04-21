export default {
  async fetch(request, env) {
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders() });
    }

    if (request.method !== "POST") {
      return withCors(json({ ok: false, error: "Method not allowed" }, 405));
    }

    const debugLogs = [];
    const log = (...parts) => {
      const line = parts.map(p => {
        try { return typeof p === "string" ? p : JSON.stringify(p); }
        catch { return String(p); }
      }).join(" ");
      debugLogs.push(line);
      console.log(line);
    };

    try {
      const body = await request.json();
      const {
        sceneId,
        providerMode = "auto",
        accuracyMode = "fast",
        imageDataUrl,
        subjectHint = null
      } = body || {};

      if (!sceneId) {
        return withCors(json({ ok: false, error: "Missing sceneId" }, 400));
      }

      if (!imageDataUrl) {
        return withCors(json({ ok: false, error: "Missing imageDataUrl" }, 400));
      }

      const sceneMeta = getSceneMeta(sceneId);
      if (!sceneMeta) {
        return withCors(json({ ok: false, error: "Unknown sceneId" }, 400));
      }

      const userImage = parseDataUrl(imageDataUrl);
      const sceneRefUrl = getSceneRefUrl(sceneId, env);
      const sceneRef = sceneRefUrl ? await fetchImageAsDataUrl(sceneRefUrl) : null;

      const finalPrompt = buildScenePrompt(sceneMeta, subjectHint);

      const providerOrder = getProviderOrder(providerMode, accuracyMode);

      log("sceneId:", sceneId);
      log("providerMode:", providerMode);
      log("accuracyMode:", accuracyMode);
      log("providerOrder:", providerOrder);
      log("sceneRefUrl:", sceneRefUrl || "none");
      if (subjectHint) log("subjectHint:", subjectHint);

      const failures = [];

      for (const providerName of providerOrder) {
        try {
          let result = null;

          if (providerName === "gemini") {
            result = await tryGemini(finalPrompt, userImage, sceneRef, env, log);
          } else if (providerName === "xai") {
            result = await tryXai(finalPrompt, userImage, env, log);
          } else if (providerName === "replicate_seedream") {
            result = await tryReplicateSeedream(finalPrompt, userImage, sceneRef, env, log);
          } else if (providerName === "replicate_flux") {
            result = await tryReplicateFlux(finalPrompt, userImage, sceneRef, env, log);
          } else if (providerName === "replicate_stability") {
            result = await tryReplicateStability(finalPrompt, userImage, env, log);
          } else {
            throw new Error(`Unknown provider: ${providerName}`);
          }

          log("SUCCESS PROVIDER:", providerName, "model:", result.model);

          return withCors(json({
            ok: true,
            provider: providerName,
            model: result.model,
            mimeType: result.mimeType,
            imageBase64: result.imageBase64,
            overlayBlankUrl: env.OVERLAY_BLANK_URL || "",
            sceneTitle: sceneMeta.title,
            debugLogs
          }));
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          failures.push({ provider: providerName, error: msg });
          log("FAILED PROVIDER:", providerName, msg);
        }
      }

      return withCors(json({
        ok: false,
        error: "All providers failed",
        failures,
        overlayBlankUrl: env.OVERLAY_BLANK_URL || "",
        sceneTitle: sceneMeta.title,
        debugLogs
      }, 502));
    } catch (err) {
      return withCors(json({
        ok: false,
        error: err instanceof Error ? err.message : String(err)
      }, 500));
    }
  }
};

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type"
  };
}

function withCors(response) {
  const headers = new Headers(response.headers);
  for (const [k, v] of Object.entries(corsHeaders())) headers.set(k, v);
  return new Response(response.body, { status: response.status, headers });
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "Content-Type": "application/json" }
  });
}

function parseDataUrl(dataUrl) {
  const m = /^data:([^;]+);base64,(.+)$/s.exec(dataUrl);
  if (!m) throw new Error("Invalid imageDataUrl");
  return {
    mimeType: m[1],
    data: m[2],
    dataUrl
  };
}

async function fetchImageAsDataUrl(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch scene reference: ${res.status}`);
  const mimeType = res.headers.get("content-type") || "image/webp";
  const buf = await res.arrayBuffer();
  const b64 = arrayBufferToBase64(buf);
  return {
    mimeType,
    data: b64,
    dataUrl: `data:${mimeType};base64,${b64}`
  };
}

function getSceneRefUrl(sceneId, env) {
  const map = {
    scene: env.SCENE_SCENE_CLEAN,
    entrance: env.SCENE_ENTRANCE_CLEAN,
    exit: env.SCENE_EXIT_CLEAN,
    loud: env.SCENE_LOUD_CLEAN,
    personal: env.SCENE_PERSONAL_CLEAN
  };
  return map[sceneId] || "";
}

function getProviderOrder(providerMode, accuracyMode) {
  if (providerMode === "gemini") return ["gemini"];
  if (providerMode === "xai") return ["xai"];
  if (providerMode === "replicate_seedream") return ["replicate_seedream", "replicate_stability"];
  if (providerMode === "replicate_flux") return ["replicate_flux", "replicate_stability"];

  if (accuracyMode === "accurate") {
    return ["replicate_seedream", "replicate_flux", "replicate_stability"];
  }

  return ["gemini", "xai", "replicate_seedream", "replicate_flux", "replicate_stability"];
}

function getSceneMeta(sceneId) {
  const map = {
    scene: {
      title: "MAKE A SCENE",
      camera: "portrait poster framing, centered, medium shot, face clearly visible",
      composition: "subject seated at diner table, expressive pose, sandwich plate visible, warm diner background",
      scenePriority: "strict diner composition accuracy"
    },
    entrance: {
      title: "MAKE AN ENTRANCE",
      camera: "portrait poster framing, centered, medium-full shot, face visible",
      composition: "subject as the lead figure at carnival/fairground, colorful background crowd, confident entrance",
      scenePriority: "strict carnival composition accuracy"
    },
    exit: {
      title: "MAKE AN EXIT",
      camera: "portrait poster framing, centered, intimate medium shot, emotional visibility",
      composition: "black-and-white airport farewell with classic blocking and foggy runway mood",
      scenePriority: "strict grayscale farewell composition accuracy"
    },
    loud: {
      title: "MAKE IT LOUD",
      camera: "portrait poster framing, centered, aggressive medium-full shot, face visible",
      composition: "newsroom confrontation, pointing gesture, clocks and telephones behind subject",
      scenePriority: "strict newsroom composition accuracy"
    },
    personal: {
      title: "MAKE IT PERSONAL",
      camera: "portrait poster framing, centered, tight dramatic medium shot, face visible",
      composition: "traditional wooden interior, sword forward, wuxia pose, strong dramatic lighting",
      scenePriority: "strict sword-duel composition accuracy"
    }
  };
  return map[sceneId] || null;
}

function buildScenePrompt(sceneMeta, subjectHint) {
  const hintBlock = subjectHint ? `
Subject placement hint from detected face:
- original upload face center x: ${subjectHint.faceCenterX}
- original upload face center y: ${subjectHint.faceCenterY}
- original upload face width ratio: ${subjectHint.faceWidthRatio}
- original upload face height ratio: ${subjectHint.faceHeightRatio}

Use this hint only to preserve visibility and centering of the uploaded person.
` : "";

  return `
Recreate the provided clean reference scene with high cinematic accuracy.

PRIORITY ORDER:
1. Scene composition accuracy
2. Camera angle and framing
3. Subject placement and visibility
4. Identity resemblance (approximate is acceptable but should still reflect the uploaded person)

STRICT SCENE REQUIREMENTS:
- Match the clean reference scene layout as closely as possible
- Match the camera angle, spatial arrangement, mood, and lighting direction
- Match the environment and background logic

SUBJECT REQUIREMENTS:
- Replace the original lead actor with the uploaded person
- The uploaded person must be clearly visible and centered in frame
- The face must be recognizable and not cropped out
- Compose for a vertical poster
- Do not hide or push the person out of frame
- The uploaded person is the only identity reference

SCENE DETAILS:
- Scene title: ${sceneMeta.title}
- Camera target: ${sceneMeta.camera}
- Composition target: ${sceneMeta.composition}
- Scene priority: ${sceneMeta.scenePriority}

TEXT / TYPOGRAPHY RULES:
- DO NOT generate any title text
- DO NOT generate any typography
- DO NOT generate any logos
- DO NOT generate any film strip frame
- DO NOT copy words from the reference scene
- Only generate the cinematic photographic scene itself

LIKELIHOOD RULES:
- Preserve visible facial structure where possible
- Scene fidelity is more important than perfect facial match
- The person should still read as the uploaded subject, not as a random person

${hintBlock}

Final framing rule:
- Keep the subject centered and clearly inside the safe poster area
- Favor portrait composition that leaves room for post-overlay title and frame
`;
}

async function tryGemini(prompt, userImage, sceneRef, env, log) {
  const models = [
    env.GEMINI_MODEL_PRIMARY,
    env.GEMINI_MODEL_FALLBACK
  ].filter(Boolean);

  if (!env.GEMINI_API_KEY) throw new Error("GEMINI_API_KEY missing");

  for (const model of models) {
    try {
      log("Trying Gemini model:", model);

      const parts = [
        { text: prompt },
        { inline_data: { mime_type: userImage.mimeType, data: userImage.data } }
      ];

      if (sceneRef) {
        parts.push({
          inline_data: { mime_type: sceneRef.mimeType, data: sceneRef.data }
        });
      }

      const res = await fetch(
        `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(env.GEMINI_API_KEY)}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            contents: [{ parts }],
            generationConfig: {
              responseModalities: ["IMAGE"]
            }
          })
        }
      );

      const text = await res.text();
      if (!res.ok) throw new Error(`Gemini ${res.status}: ${text.slice(0, 1000)}`);

      const data = JSON.parse(text);
      const imagePart =
        data?.candidates?.[0]?.content?.parts?.find(p => p?.inline_data?.data || p?.inlineData?.data);

      const inlineData = imagePart?.inline_data || imagePart?.inlineData;
      if (!inlineData?.data) throw new Error("Gemini returned no image");

      return {
        provider: "gemini",
        model,
        mimeType: inlineData.mime_type || inlineData.mimeType || "image/png",
        imageBase64: inlineData.data
      };
    } catch (e) {
      log("Gemini failed:", model, e instanceof Error ? e.message : String(e));
    }
  }

  throw new Error("All Gemini models failed");
}

async function tryXai(prompt, userImage, env, log) {
  const models = [
    env.XAI_MODEL_PRIMARY,
    env.XAI_MODEL_FALLBACK
  ].filter(Boolean);

  if (!env.XAI_API_KEY) throw new Error("XAI_API_KEY missing");

  for (const model of models) {
    try {
      log("Trying xAI model:", model);

      const res = await fetch("https://api.x.ai/v1/images/edits", {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${env.XAI_API_KEY}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          model,
          prompt: `${prompt}

Important note:
This provider is scene-oriented and may not preserve likeness exactly.
Do not generate any text or poster frame.
`,
          image_url: userImage.dataUrl
        })
      });

      const text = await res.text();
      if (!res.ok) throw new Error(`xAI ${res.status}: ${text.slice(0, 1000)}`);

      const data = JSON.parse(text);
      const imageUrl = data?.data?.[0]?.url;
      const b64 = data?.data?.[0]?.b64_json;

      if (b64) {
        return {
          provider: "xai",
          model,
          mimeType: "image/png",
          imageBase64: b64
        };
      }

      if (!imageUrl) throw new Error("xAI returned no image");

      const fetched = await fetch(imageUrl);
      if (!fetched.ok) throw new Error(`xAI output fetch ${fetched.status}`);
      const mimeType = fetched.headers.get("content-type") || "image/png";
      const buf = await fetched.arrayBuffer();

      return {
        provider: "xai",
        model,
        mimeType,
        imageBase64: arrayBufferToBase64(buf)
      };
    } catch (e) {
      log("xAI failed:", model, e instanceof Error ? e.message : String(e));
    }
  }

  throw new Error("All xAI models failed");
}

async function tryReplicateSeedream(prompt, userImage, sceneRef, env, log) {
  const entry = env.REPLICATE_MODEL_BYTEDANCE;
  if (!entry) throw new Error("REPLICATE_MODEL_BYTEDANCE missing");

  log("Trying Replicate entry:", entry);
  const input = {
    prompt,
    image_input: [userImage.dataUrl, sceneRef?.dataUrl].filter(Boolean),
    size: "2K",
    aspect_ratio: "3:4"
  };

  return await runReplicate(entry, input, env.REPLICATE_API_KEY, log);
}

async function tryReplicateFlux(prompt, userImage, sceneRef, env, log) {
  const entry = env.REPLICATE_MODEL_FLUX;
  if (!entry) throw new Error("REPLICATE_MODEL_FLUX missing");

  log("Trying Replicate entry:", entry);
  const input = {
    prompt,
    input_image: userImage.dataUrl,
    reference_image: sceneRef?.dataUrl,
    strength: 0.9,
    aspect_ratio: "3:4"
  };

  return await runReplicate(entry, input, env.REPLICATE_API_KEY, log);
}

async function tryReplicateStability(prompt, userImage, env, log) {
  const entry = env.REPLICATE_MODEL_STABILITY;
  if (!entry) throw new Error("REPLICATE_MODEL_STABILITY missing");

  log("Trying Replicate entry:", entry);
  const input = {
    prompt,
    image: userImage.dataUrl,
    strength: 0.82
  };

  return await runReplicate(entry, input, env.REPLICATE_API_KEY, log);
}

async function runReplicate(entry, input, apiKey, log) {
  if (!apiKey) throw new Error("REPLICATE_API_KEY missing");

  const [owner, name] = entry.split("/");
  if (!owner || !name) {
    throw new Error(`Replicate entry must be owner/name: ${entry}`);
  }

  const createRes = await fetch(
    `https://api.replicate.com/v1/models/${encodeURIComponent(owner)}/${encodeURIComponent(name)}/predictions`,
    {
      method: "POST",
      headers: {
        "Authorization": `Token ${apiKey}`,
        "Content-Type": "application/json",
        "Prefer": "wait=20"
      },
      body: JSON.stringify({ input })
    }
  );

  const createText = await createRes.text();
  if (!createRes.ok) throw new Error(`Replicate create ${createRes.status}: ${createText.slice(0, 1000)}`);

  let prediction = JSON.parse(createText);

  if (!["succeeded", "failed", "canceled"].includes(prediction.status)) {
    let pollCount = 0;
    while (pollCount < 20) {
      pollCount += 1;
      await sleep(1500);

      const pollRes = await fetch(prediction.urls.get, {
        headers: { "Authorization": `Token ${apiKey}` }
      });

      const pollText = await pollRes.text();
      if (!pollRes.ok) throw new Error(`Replicate poll ${pollRes.status}: ${pollText.slice(0, 1000)}`);

      prediction = JSON.parse(pollText);
      log("Replicate poll status:", prediction.status);

      if (["succeeded", "failed", "canceled"].includes(prediction.status)) {
        break;
      }
    }
  }

  if (prediction.status !== "succeeded") {
    throw new Error(`Replicate status=${prediction.status || "unknown"}`);
  }

  let outputUrl = null;
  if (typeof prediction.output === "string") outputUrl = prediction.output;
  if (Array.isArray(prediction.output) && prediction.output[0]) outputUrl = prediction.output[0];
  if (!outputUrl) throw new Error("Replicate returned no output URL");

  const fetched = await fetch(outputUrl);
  if (!fetched.ok) throw new Error(`Replicate output fetch ${fetched.status}`);
  const mimeType = fetched.headers.get("content-type") || "image/jpeg";
  const buf = await fetched.arrayBuffer();

  return {
    provider: "replicate",
    model: entry,
    mimeType,
    imageBase64: arrayBufferToBase64(buf)
  };
}

function arrayBufferToBase64(buffer) {
  const bytes = new Uint8Array(buffer);
  let binary = "";
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
  }
  return btoa(binary);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
