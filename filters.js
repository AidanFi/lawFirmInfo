function applyFilters(firms, filters, userData) {
  return firms.filter(firm => {
    if (filters.search) {
      const q = filters.search.toLowerCase();
      if (!firm.name.toLowerCase().includes(q)) return false;
    }
    if (!filters.referralScores.includes(firm.referralScore)) return false;
    if (filters.practiceAreas.length > 0) {
      const match = filters.practiceAreas.some(pa => firm.practiceAreas.includes(pa));
      if (!match) return false;
    }
    if (filters.city && firm.address.city !== filters.city) return false;
    if (filters.county && firm.address.county !== filters.county) return false;
    const status = (userData.status && userData.status[firm.id]) || "uncontacted";
    if (!filters.contactStatuses.includes(status)) return false;
    if (filters.starredOnly && !((userData.starred || {})[firm.id])) return false;
    if (filters.hasWebsite && !firm.website) return false;
    if (filters.hasPracticeArea && (!firm.practiceAreas || firm.practiceAreas.length === 0)) return false;
    if (filters.hasContact && !firm.phone && !firm.email) return false;
    return true;
  });
}

if (typeof module !== "undefined") module.exports = { applyFilters };
