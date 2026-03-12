const fs = require('fs');
const {
    Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
    HeadingLevel, AlignmentType, BorderStyle, WidthType, ShadingType,
    LevelFormat
} = require('docx');

const border = { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" };
const borders = { top: border, bottom: border, left: border, right: border };
const cellMargins = { top: 80, bottom: 80, left: 120, right: 120 };

function text(t, opts = {}) {
    return new TextRun({ text: t, font: "Arial", size: 22, ...opts });
}

function para(runs, opts = {}) {
    if (typeof runs === 'string') runs = [text(runs)];
    return new Paragraph({ children: runs, spacing: { after: 160 }, ...opts });
}

function heading(t, level) {
    return new Paragraph({
        heading: level,
        children: [new TextRun({ text: t, font: "Arial", bold: true, size: level === HeadingLevel.HEADING_1 ? 32 : 26 })],
        spacing: { before: 280, after: 160 }
    });
}

function cell(t, opts = {}) {
    return new TableCell({
        borders,
        margins: cellMargins,
        width: opts.width ? { size: opts.width, type: WidthType.DXA } : undefined,
        shading: opts.shading ? { fill: opts.shading, type: ShadingType.CLEAR } : undefined,
        children: [para(typeof t === 'string' ? [text(t, opts.textOpts || {})] : t, { spacing: { after: 0 } })]
    });
}

const doc = new Document({
    styles: {
        default: { document: { run: { font: "Arial", size: 22 } } },
        paragraphStyles: [
            { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
              run: { size: 32, bold: true, font: "Arial", color: "1a1d27" },
              paragraph: { spacing: { before: 280, after: 200 }, outlineLevel: 0 } },
            { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
              run: { size: 26, bold: true, font: "Arial", color: "333333" },
              paragraph: { spacing: { before: 240, after: 160 }, outlineLevel: 1 } },
        ]
    },
    numbering: {
        config: [
            {
                reference: "bullets",
                levels: [{
                    level: 0, format: LevelFormat.BULLET, text: "\u2022",
                    alignment: AlignmentType.LEFT,
                    style: { paragraph: { indent: { left: 720, hanging: 360 } } }
                }]
            },
            {
                reference: "numbers",
                levels: [{
                    level: 0, format: LevelFormat.DECIMAL, text: "%1.",
                    alignment: AlignmentType.LEFT,
                    style: { paragraph: { indent: { left: 720, hanging: 360 } } }
                }]
            }
        ]
    },
    sections: [{
        properties: {
            page: {
                size: { width: 12240, height: 15840 },
                margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 }
            }
        },
        children: [
            // Title
            new Paragraph({
                alignment: AlignmentType.LEFT,
                spacing: { after: 80 },
                children: [new TextRun({ text: "Growth Pod Analytics Memo", font: "Arial", size: 40, bold: true })]
            }),
            new Paragraph({
                spacing: { after: 80 },
                children: [text("NewtonX Expert Supply Analysis — Jan–Dec 2024", { color: "666666", size: 22 })]
            }),
            new Paragraph({
                spacing: { after: 400 },
                border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: "6c72cb" } },
                children: [text("Prepared for: Growth PM, Marketing Lead, VP of Product", { color: "999999", size: 20 })]
            }),

            // 1. Executive Summary
            heading("1. Executive Summary", HeadingLevel.HEADING_1),
            para([
                text("Over 2024, NewtonX acquired 12,000 expert signups across six channels, of which 1,348 (11.2%) reached activation (first paid engagement). "),
                text("The biggest funnel bottleneck is the Signup → Profile Started transition, where 29% of experts are lost. ", { bold: true }),
                text("Referral (23.5% activation) and LinkedIn Outreach (16.4%) dramatically outperform paid search (7.1%) and paid social (3.7%) on both conversion and cost efficiency. "),
                text("Our top recommendation is to scale the referral program and LinkedIn outreach while aggressively optimizing or reducing paid social spend, which currently runs a 0.10x LTV:CAC — well below breakeven.", { bold: true }),
            ]),

            // 2. Funnel Insights
            heading("2. Funnel Insights", HeadingLevel.HEADING_1),
            para("The full funnel tells a clear story of progressive attrition with two critical chokepoints:"),

            new Table({
                width: { size: 9360, type: WidthType.DXA },
                columnWidths: [2800, 1600, 1600, 1700, 1660],
                rows: [
                    new TableRow({
                        children: [
                            cell("Stage", { shading: "E8EAF6", textOpts: { bold: true } }),
                            cell("Experts", { shading: "E8EAF6", textOpts: { bold: true } }),
                            cell("From Previous", { shading: "E8EAF6", textOpts: { bold: true } }),
                            cell("From Signup", { shading: "E8EAF6", textOpts: { bold: true } }),
                            cell("Drop-off", { shading: "E8EAF6", textOpts: { bold: true } }),
                        ]
                    }),
                    ...([
                        ["Signup", "12,000", "—", "100%", "—"],
                        ["Profile Started", "8,529", "71.1%", "71.1%", "3,471 lost"],
                        ["Profile Completed", "6,055", "71.0%", "50.5%", "2,474 lost"],
                        ["Verif. Submitted", "3,734", "61.7%", "31.1%", "2,321 lost"],
                        ["Verified", "3,206", "85.9%", "26.7%", "528 rejected"],
                        ["Activated", "1,348", "42.0%", "11.2%", "1,858 lost"],
                    ]).map(row => new TableRow({
                        children: row.map((c, i) => cell(c, { width: [2800,1600,1600,1700,1660][i] }))
                    }))
                ]
            }),

            para(""),
            para([text("Hypotheses for the key drop-offs:", { bold: true })]),
            para([text("Signup → Profile Started (29% loss): ", { bold: true }), text("Onboarding friction. Experts may not understand the value proposition or find the profile form daunting. 3,235 experts literally never clicked beyond signup. This is the single largest addressable opportunity.")]),
            para([text("Profile Completed → Verification Submitted (39% loss): ", { bold: true }), text("Experts may not realize they need to submit for verification, or the verification requirements feel burdensome. This suggests a UX/messaging gap between completing a profile and understanding the next step.")]),
            para([text("Verified → Activated (58% loss): ", { bold: true }), text("The 1,858 verified-but-not-activated experts represent supply that cleared our quality bar but never got matched. This is likely a demand-side constraint — we may not have enough projects matching their expertise areas.")]),

            // 3. Channel Recommendations
            heading("3. Channel Recommendations", HeadingLevel.HEADING_1),
            para([text("Scale aggressively:", { bold: true, color: "16a34a" })]),
            para([text("Referral: ", { bold: true }), text("23.5% activation rate at zero acquisition cost. The referral_program campaign alone drove 2,046 signups with the highest quality experts (measured by payout volume). Invest in referral incentives, make the referral flow more prominent, and consider tiered rewards.")]),
            para([text("LinkedIn Outreach: ", { bold: true }), text("16.4% activation at $560 CPA with a 1.17x LTV:CAC — the only paid channel above breakeven. The outreach_finance (18.9% activation), outreach_tech (16.7%), and outreach_healthcare (15.4%) campaigns are all strong. Scale budget here.")]),

            para([text("Optimize:", { bold: true, color: "d97706" })]),
            para([text("Paid Search: ", { bold: true }), text("7.1% activation rate with $4,163 CPA and 0.15x LTV:CAC. The brand_search_q1 campaign is a necessary defensive spend, but generic/competitor campaigns need tighter targeting or should be paused.")]),
            para([text("Organic: ", { bold: true }), text("13.9% activation at zero cost. Invest in SEO and content marketing to grow this channel — it has the second-best quality after referral.")]),

            para([text("Reduce or pause:", { bold: true, color: "dc2626" })]),
            para([text("Paid Social: ", { bold: true }), text("3.7% activation at $6,154 CPA and 0.10x LTV:CAC. This channel is burning ~$474K/year to produce 77 activated experts. Even with optimization, the unit economics are dire. Recommend reducing to retargeting-only and reallocating budget to LinkedIn outreach.")]),

            // 4. Data Quality Flags
            heading("4. Data Quality Flags", HeadingLevel.HEADING_1),
            para([text("Unknown source (~15%): ", { bold: true }), text("1,841 experts (15.3%) have signup_source = 'unknown'. These likely result from UTM parameter stripping (ad blockers, redirect chains, or iOS privacy features). Recommendation: implement server-side tracking and first-party cookies to reduce this bucket. The unknown segment also has an anomalously low 2.0% activation rate, which may indicate bot signups or tracking-only issues.")]),
            para([text("Out-of-order timestamps: ", { bold: true }), text("The data notes warn of occasional timestamp ordering issues. Our dbt models handle this by using event_name (not timestamp order) to define funnel stages, taking the MIN timestamp per stage. This is defensive but means we cannot reliably compute precise stage-to-stage durations.")]),
            para([text("Right-censoring in Q4 cohorts: ", { bold: true }), text("Activation rates for Nov–Dec 2024 cohorts appear low because those experts haven't had a full 60-day window yet. The Q3 vs Q4 comparison should be interpreted cautiously — the Q4 dip may partially be a data maturity issue rather than a true performance decline.")]),
            para([text("Null UTM fields: ", { bold: true }), text("utm_campaign is null for 3,873 experts (32%). This is expected for organic and unknown sources, but any nulls in paid channel experts would indicate broken campaign tracking.")]),

            // 5. Suggested Experiments
            heading("5. Suggested Experiments", HeadingLevel.HEADING_1),

            para([text("Experiment 1: Simplified Onboarding (Signup → Profile Started)", { bold: true })]),
            para("Hypothesis: Reducing the initial profile form to 3 essential fields (name, industry, expertise) will increase Signup → Profile Started conversion."),
            para("Design: A/B test — 50/50 split of new signups. Control sees current full form; variant sees abbreviated form with a 'complete later' option."),
            para("Success metric: Profile Started rate (target: +10pp, from 71% to 81%). Guardrails: Profile Completed rate should not drop by more than 5pp; verification approval rate should remain within 2pp."),
            para(""),

            para([text("Experiment 2: Referral Incentive Boost", { bold: true })]),
            para("Hypothesis: Doubling the referral reward (or adding a two-sided reward) will increase referral volume without degrading quality."),
            para("Design: Time-based test — 4 weeks baseline, 4 weeks with enhanced incentive. Track via referral_program campaign tag."),
            para("Success metric: 25%+ increase in referral signups while maintaining >20% activation rate. Guardrails: Cost per activated referral should stay under $200 (including incentive payouts); activation rate must not drop below 18%."),
            para(""),

            para([text("Experiment 3: Post-Verification Nudge Sequence", { bold: true })]),
            para("Hypothesis: A targeted email/SMS sequence to verified-but-not-activated experts, highlighting available projects in their industry, will increase the Verified → Activated conversion."),
            para("Design: A/B test — 50/50 of newly verified experts. Control receives current communications; variant receives a 3-touch sequence at days 3, 7, and 14 post-verification."),
            para("Success metric: Verified → Activated rate (target: +8pp, from 42% to 50%). Guardrails: Unsubscribe rate should not exceed 5%; first engagement quality (measured by payout amount) should remain within 10% of baseline."),
        ]
    }]
});

Packer.toBuffer(doc).then(buffer => {
    fs.writeFileSync('/home/claude/newtonx_takehome/memo/growth_pod_memo.docx', buffer);
    console.log('Memo created successfully');
});
