# Security

This is an independently maintained solution accelerator. It is **not** a
Microsoft product, and it is not covered by the Microsoft Security Response
Center (MSRC) or any Microsoft support agreement.

## Reporting a vulnerability

Please do not report security vulnerabilities through public GitHub issues.

Use [private vulnerability reporting](https://docs.github.com/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
on this repository: **Security → Report a vulnerability**. That channel is
private to the maintainers.

Please include enough detail to reproduce: the affected file and version, what
an attacker can achieve, and the steps you took.

This is a sample codebase maintained on a best-effort basis. There is no
guaranteed response time and no bug bounty.

## Reporting a vulnerability in a Microsoft product

If you have found a vulnerability in Azure AI Foundry, Power BI, Microsoft
Graph or any other Microsoft service — as opposed to in this sample code —
report it to MSRC at [https://aka.ms/SECURITY.md](https://aka.ms/SECURITY.md).
Do not report it here.

## Before you deploy this

This accelerator grants an autonomous agent permission to act on a Power BI
estate. Read [`docs/TechnicalArchitecture.md`](docs/TechnicalArchitecture.md)
and set your own policy limits before pointing it at anything you care about.
The defaults are conservative, not authoritative.
