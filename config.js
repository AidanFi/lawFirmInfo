const PRACTICE_COMPATIBILITY = {
  "Personal Injury": {
    competitor: ["Personal Injury", "Litigation", "Trial Law", "Wrongful Death"],
    high: ["Family Law", "Criminal Defense", "Estate Planning", "Probate",
           "Workers' Compensation", "Bankruptcy", "Social Security Disability"],
    medium: ["Business Law", "Real Estate", "Immigration", "Employment Law",
             "Medical Malpractice", "Insurance Defense"],
  },
  "Family Law": {
    competitor: ["Family Law", "Divorce", "Child Custody"],
    high: ["Criminal Defense", "Estate Planning", "Personal Injury", "Bankruptcy", "Immigration"],
    medium: ["Business Law", "Real Estate", "Employment Law"],
  },
  "Criminal Defense": {
    competitor: ["Criminal Defense", "DUI", "Criminal Law"],
    high: ["Personal Injury", "Family Law", "Immigration", "Civil Litigation"],
    medium: ["Business Law", "Employment Law"],
  },
  "Estate Planning": {
    competitor: ["Estate Planning", "Probate", "Wills & Trusts"],
    high: ["Family Law", "Business Law", "Real Estate", "Bankruptcy", "Elder Law"],
    medium: ["Personal Injury", "Tax Law", "Immigration"],
  },
  "Business Law": {
    competitor: ["Business Law", "Corporate Law", "Commercial Litigation"],
    high: ["Real Estate", "Employment Law", "Tax Law", "Intellectual Property", "Bankruptcy"],
    medium: ["Civil Litigation", "Immigration"],
  },
  "Real Estate": {
    competitor: ["Real Estate", "Property Law"],
    high: ["Business Law", "Estate Planning", "Environmental Law", "Zoning"],
    medium: ["Family Law", "Civil Litigation"],
  },
  "Immigration": {
    competitor: ["Immigration", "Immigration Law"],
    high: ["Criminal Defense", "Employment Law", "Family Law"],
    medium: ["Civil Rights", "Business Law"],
  },
  "Bankruptcy": {
    competitor: ["Bankruptcy", "Debt Relief"],
    high: ["Business Law", "Real Estate", "Tax Law", "Employment Law"],
    medium: ["Family Law", "Civil Litigation"],
  },
};

function calculateReferralScore(practiceAreas, myPractice) {
  const config = PRACTICE_COMPATIBILITY[myPractice] || PRACTICE_COMPATIBILITY["Personal Injury"];
  for (const area of practiceAreas) {
    if (config.competitor.some(c => area.toLowerCase().includes(c.toLowerCase()))) return "competitor";
  }
  for (const area of practiceAreas) {
    if (config.high.some(c => area.toLowerCase().includes(c.toLowerCase()))) return "high";
  }
  for (const area of practiceAreas) {
    if (config.medium.some(c => area.toLowerCase().includes(c.toLowerCase()))) return "medium";
  }
  return "low";
}

if (typeof module !== "undefined") module.exports = { PRACTICE_COMPATIBILITY, calculateReferralScore };
