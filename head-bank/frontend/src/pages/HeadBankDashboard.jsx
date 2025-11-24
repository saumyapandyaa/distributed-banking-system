// src/pages/HeadBankDashboard.jsx
import React, { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { getBanks, searchBank } from "../api/headBankApi";

export default function HeadBankDashboard() {
  const navigate = useNavigate();

  const [banks, setBanks] = useState([]);
  const [searchId, setSearchId] = useState("");

  useEffect(() => {
    loadBanks();
  }, []);

  async function loadBanks() {
    try {
      const res = await getBanks();
      setBanks(res.data || []);
    } catch (err) {
      console.error("Failed to load banks", err);
    }
  }

  const handleSearch = async () => {
    if (!searchId.trim()) {
      loadBanks();
      return;
    }

    try {
      const res = await searchBank(searchId.trim());
      setBanks([res.data]);
    } catch {
      alert("Bank not found");
    }
  };

  return (
    <div className="min-h-screen flex bg-[#f3f1f8]">

      {/* SIDEBAR */}
      <div className="w-64 bg-white p-6 shadow-lg flex flex-col gap-5">
        <h2 className="text-xl font-bold mb-2">Actions</h2>

        <button
          onClick={() => navigate("/create-bank")}
          className="bg-blue-600 text-white px-4 py-2 rounded shadow hover:bg-blue-700 transition"
        >
          + Create Bank
        </button>

        <button
          onClick={() => navigate("/delete-bank")}
          className="bg-red-600 text-white px-4 py-2 rounded shadow hover:bg-red-700 transition"
        >
          Delete Bank
        </button>

        {/* FIXED LOGOUT */}
        <button
          onClick={() => {
            localStorage.removeItem("headbank_token");
            window.location.href = "http://localhost:3001";
          }}
          className="mt-auto bg-gray-700 text-white px-4 py-2 rounded shadow hover:bg-gray-800 transition"
        >
          Logout
        </button>
      </div>

      {/* MAIN CONTENT */}
      <div className="flex-1 p-10">

        {/* TOP BAR */}
        <div className="flex justify-between items-center mb-8">
          <h1 className="text-3xl font-bold">Head Bank Dashboard</h1>

          <button
            onClick={loadBanks}
            className="bg-green-600 text-white px-4 py-2 rounded shadow hover:bg-green-700 transition"
          >
            Refresh
          </button>
        </div>

        {/* SEARCH BAR */}
        <div className="flex gap-3 mb-6">
          <input
            className="border p-3 rounded w-80 shadow-sm"
            placeholder="Search by Bank ID"
            value={searchId}
            onChange={(e) => setSearchId(e.target.value)}
          />

          <button
            onClick={handleSearch}
            className="bg-gray-800 text-white px-5 py-2 rounded shadow hover:bg-black transition"
          >
            Search
          </button>
        </div>

        {/* BANK LIST */}
        <div className="bg-white p-6 rounded-xl shadow">
          <h2 className="text-2xl font-bold mb-4">Existing Banks</h2>

          <table className="w-full border-collapse">
            <thead>
              <tr className="border-b text-gray-600">
                <th className="pb-3">Bank ID</th>
                <th className="pb-3">Name</th>
                <th className="pb-3">Base URL</th>
                <th className="pb-3">Status</th>
              </tr>
            </thead>

            <tbody>
              {banks.map((b) => (
                <tr key={b.bank_id} className="border-b hover:bg-gray-50 transition">
                  <td className="py-3">{b.bank_id}</td>
                  <td>{b.name}</td>
                  <td>{b.base_url}</td>
                  <td className={b.status === "active" ? "text-green-700 font-semibold" : "text-red-700 font-semibold"}>
                    {b.status}
                  </td>
                </tr>
              ))}
            </tbody>

          </table>
        </div>
      </div>
    </div>
  );
}
