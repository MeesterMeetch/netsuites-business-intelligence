#!/usr/bin/env node
import readline from "node:readline";
import { spawn } from "node:child_process";

const run = (cmd) => {
  const [bin, ...args] = cmd.split(" ");
  const p = spawn(bin, args, { stdio: "inherit", shell: true });
  p.on("exit", (code) => process.exit(code ?? 0));
};

const items = [
  ["Backfill (365d RESET) — Essential Electric", "npm run backfill:ees:reset"],
  ["Backfill (365d RESET) — CBG", "npm run backfill:cbg:reset"],
  ["Backfill (365d RESET) — United Fuses", "npm run backfill:uf:reset"],
  ["Backfill (30d) — Essential Electric", "npm run backfill:ees"],
  ["Backfill (30d) — CBG", "npm run backfill:cbg"],
  ["Backfill (30d) — United Fuses", "npm run backfill:uf"],
  ["Backfill (30d) — ALL stores", "npm run backfill:all"],
  ["KPIs — Top SKUs (30d)", "npm run kpis:top-skus"],
  ["KPIs — Bottom SKUs (30d)", "npm run kpis:bottom-skus"],
  ["KPIs — Daily (14d)", "npm run kpis:daily"],
  ["KPIs — Rolling", "npm run kpis:rolling"],
  ["Deploy worker", "npm run deploy"],
  ["Tail logs", "npm run tail"],
  ["Commit + Push + Deploy (ship)", "npm run ship"]
];

console.log("\n🧭  BI menu — pick a number:\n");
items.forEach(([label], i) => console.log(`${String(i + 1).padStart(2, " ")}) ${label}`));
console.log(" q) Quit\n");

const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
rl.question("Choice: ", (ans) => {
  if (ans.toLowerCase() === "q") return process.exit(0);
  const idx = Number(ans) - 1;
  if (Number.isNaN(idx) || idx < 0 || idx >= items.length) {
    console.error("Invalid choice.");
    process.exit(1);
  }
  const [, cmd] = items[idx];
  console.log(`\n→ Running: ${cmd}\n`);
  rl.close();
  run(cmd);
});
