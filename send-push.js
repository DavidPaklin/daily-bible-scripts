import { cert, initializeApp } from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";
import { getMessaging } from "firebase-admin/messaging";
import { DateTime } from "luxon";

const TZ = "Europe/Kyiv";
const WINDOW_MINUTES = 2;
const BATCH_SIZE = 500;
const CLEAN_INVALID = true;
const NOTIFICATIONS_COLLECTION = "notifications";

const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);

initializeApp({
  credential: cert(serviceAccount),
});
const db = getFirestore();
const messaging = getMessaging();

function scheduledDateTimesForToday(hhmm, now) {
  const [hh, mm] = hhmm.split(":").map((s) => parseInt(s, 10));
  const candidates = [];
  const base = now.set({ hour: hh, minute: mm, second: 0, millisecond: 0 });
  candidates.push(base.minus({ days: 1 }));
  candidates.push(base);
  candidates.push(base.plus({ days: 1 }));
  return candidates;
}

function isWithinWindow(now, whenArray, windowMinutes) {
  for (const hhmm of whenArray || []) {
    if (!/^\d{1,2}:\d{2}$/.test(hhmm)) continue;
    const candidates = scheduledDateTimesForToday(hhmm, now);
    for (const dt of candidates) {
      const diff = Math.abs(now.diff(dt, "minutes").minutes);
      if (diff <= windowMinutes) return true;
    }
  }
  return false;
}

async function removeTokenFromDoc(docRef, tokenToRemove) {
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(docRef);
    if (!snap.exists) return;
    const data = snap.data();
    const tokensArr = Array.isArray(data.tokens) ? data.tokens : [];
    const filtered = tokensArr.filter((t) => t?.token !== tokenToRemove);
    if (filtered.length === tokensArr.length) return;
    tx.update(docRef, { tokens: filtered });
  });
}

async function run() {
  const now = DateTime.now().setZone(TZ);
  console.log(`Now (${TZ}):`, now.toISO());

  const q = db
    .collection(NOTIFICATIONS_COLLECTION)
    .where("enabled", "==", true);
  const snap = await q.get();

  if (snap.empty) {
    console.log("No enabled notifications documents.");
    return;
  }

  const tokensSet = new Set();
  const tokenOrigin = new Map();
  for (const doc of snap.docs) {
    const data = doc.data();
    const whenArray = data.when;
    if (!Array.isArray(whenArray) || whenArray.length === 0) continue;

    if (!isWithinWindow(now, whenArray, WINDOW_MINUTES)) continue;

    const tokensField = Array.isArray(data.tokens) ? data.tokens : [];
    for (const entry of tokensField) {
      if (!entry || !entry.token) continue;
      const t = entry.token;
      if (!tokensSet.has(t)) {
        tokensSet.add(t);
        tokenOrigin.set(t, doc.ref);
      }
    }
  }

  const tokens = Array.from(tokensSet);
  if (tokens.length === 0) {
    console.log("No tokens to send for current window.");
    return;
  }

  console.log("Total tokens to send:", tokens.length);

  for (let i = 0; i < tokens.length; i += BATCH_SIZE) {
    const batch = tokens.slice(i, i + BATCH_SIZE);
    const message = {
      tokens: batch,
      data: {
        title: "Нагадування!",
        body: "Не забудь почитати Біблію :)",
      },
    };

    try {
      const res = await messaging.sendEachForMulticast(message);
      console.log(
        `Batch ${i / BATCH_SIZE + 1}: success ${res.successCount}, failures ${
          res.failureCount
        }`
      );
      res.responses.forEach((r, idx) => {
        if (!r.success) {
          const failedToken = batch[idx];
          console.log(
            ` -> token failed: ${failedToken} →`,
            r.error?.code ?? r.error?.message
          );
          if (CLEAN_INVALID) {
            const errCode = r.error?.code || "";
            const shouldRemove =
              [
                "messaging/registration-token-not-registered",
                "messaging/invalid-registration-token",
                "messaging/invalid-argument",
              ].includes(errCode) ||
              (r.error &&
                /not-registered|unregistered|gone/i.test(
                  String(r.error.message || "")
                ));

            if (shouldRemove) {
              const docRef = tokenOrigin.get(failedToken);
              if (docRef) {
                removeTokenFromDoc(docRef, failedToken).catch((e) => {
                  console.error("Failed to remove token from doc:", e);
                });
              }
            }
          }
        } else {
          const okToken = batch[idx];
          const docRef = tokenOrigin.get(okToken);
          if (docRef) {
            db.runTransaction(async (tx) => {
              const s = await tx.get(docRef);
              if (!s.exists) return;
              const d = s.data();
              const arr = Array.isArray(d.tokens) ? d.tokens : [];
              const newArr = arr.map((e) => {
                if (e?.token === okToken)
                  return { ...e, lastActiveAt: new Date().toISOString() };
                return e;
              });
              tx.update(docRef, { tokens: newArr });
            }).catch((e) => {});
          }
        }
      });
    } catch (err) {
      console.error("FCM send error:", err);
    }
  }

  console.log("Done.");
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
