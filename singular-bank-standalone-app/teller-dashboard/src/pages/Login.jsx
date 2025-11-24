// src/pages/Login.jsx
import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import { adminLogin } from "../api/tellerApi";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/card";
import { Input } from "../components/ui/input";
import { Label } from "../components/ui/label";
import { Button } from "../components/ui/button";

export default function Login() {
  const [adminId, setAdminId] = useState("");
  const [password, setPassword] = useState("");
  const navigate = useNavigate();

  const handleLogin = async () => {
    try {
      const res = await adminLogin(adminId, password);

      localStorage.setItem("token", res.data.access_token);
      localStorage.setItem("admin_id", adminId);

      alert("Login successful!");
      navigate("/ledger");
    } catch (err) {
      console.error("Login failed:", err);
      alert("Invalid Admin ID or Password");
    }
  };

  return (
    <div className="relative min-h-screen overflow-hidden bg-gradient-to-br from-slate-50 via-white to-slate-100 px-6 py-12">
      <div className="pointer-events-none absolute left-10 top-20 h-72 w-72 rounded-full bg-indigo-200/40 blur-3xl" />
      <div className="pointer-events-none absolute bottom-10 right-10 h-80 w-80 rounded-full bg-sky-200/50 blur-3xl" />

      <div className="relative mx-auto grid w-full max-w-6xl grid-cols-1 gap-12 lg:grid-cols-2">
        <div className="flex flex-col justify-center space-y-6">
          <span className="inline-flex w-fit items-center gap-2 rounded-full border border-slate-200 px-4 py-1 text-xs font-semibold uppercase tracking-widest text-slate-500">
            Teller Console
          </span>
          <div className="space-y-4">
            <h1 className="text-4xl font-semibold text-slate-900">
              Welcome back to the Singular Bank Teller dashboard
            </h1>
            <p className="text-base text-slate-600">
              Keep an eye on every branch interaction, approve requests faster,
              and switch between customer dashboards instantly.
            </p>
          </div>

          <div className="grid grid-cols-3 gap-4 text-sm text-slate-500">
            <div className="rounded-2xl border border-slate-200/70 p-4">
              <p className="text-xs uppercase tracking-widest text-slate-400">
                Active users
              </p>
              <p className="mt-2 text-2xl font-semibold text-slate-900">128</p>
            </div>
            <div className="rounded-2xl border border-slate-200/70 p-4">
              <p className="text-xs uppercase tracking-widest text-slate-400">
                Avg. handling time
              </p>
              <p className="mt-2 text-2xl font-semibold text-slate-900">3m 14s</p>
            </div>
            <div className="rounded-2xl border border-slate-200/70 p-4">
              <p className="text-xs uppercase tracking-widest text-slate-400">
                Satisfaction
              </p>
              <p className="mt-2 text-2xl font-semibold text-slate-900">98%</p>
            </div>
          </div>
        </div>

        <Card className="backdrop-blur">
          <CardHeader>
            <CardTitle>Admin Login</CardTitle>
            <p className="text-sm text-muted-foreground">
              Use your teller credentials to access the smart ledger.
            </p>
          </CardHeader>
          <CardContent className="space-y-5">
            <div className="space-y-2">
              <Label htmlFor="adminId">Admin ID</Label>
              <Input
                id="adminId"
                placeholder="e.g. ADMIN1"
                value={adminId}
                onChange={(e) => setAdminId(e.target.value)}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="password">Password</Label>
              <Input
                id="password"
                type="password"
                placeholder="Enter password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
              />
            </div>

            <Button className="w-full" onClick={handleLogin}>
              Sign in
            </Button>

            <Button
              variant="ghost"
              className="w-full text-sm text-slate-600 hover:text-slate-900"
              onClick={() => {
                window.location.href = "http://localhost:3001";
              }}
            >
              Switch to Head Bank →
            </Button>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
