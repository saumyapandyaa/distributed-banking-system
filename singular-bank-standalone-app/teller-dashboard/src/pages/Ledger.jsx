import { useEffect, useState } from "react";
import { getUsers, deleteUser } from "../api/tellerApi";
import { useNavigate } from "react-router-dom";
import { FaTrash, FaPlus, FaSignOutAlt } from "react-icons/fa";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "../components/ui/card";
import { Button } from "../components/ui/button";
import { ScrollArea } from "../components/ui/scroll-area";
import { Badge } from "../components/ui/badge";

export default function Ledger() {
  const [users, setUsers] = useState([]);
  const navigate = useNavigate();

  const loadUsers = () => {
    getUsers().then((res) => setUsers(res.data));
  };

  useEffect(() => {
    loadUsers();
  }, []);

  const handleDelete = async (userId) => {
    if (!window.confirm("Delete this user?")) return;
    await deleteUser(userId);
    loadUsers();
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 via-white to-slate-100 px-4 py-10">
      <div className="mx-auto max-w-6xl space-y-6">
        <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <p className="text-sm uppercase tracking-[0.35em] text-muted-foreground">
              Ledger
            </p>
            <h1 className="text-4xl font-semibold text-slate-900">
              Customer directory
            </h1>
          </div>

          <div className="flex flex-wrap gap-3">
            <Button
              onClick={() => navigate("/add-user")}
              className="inline-flex items-center gap-2"
            >
              <FaPlus /> Add user
            </Button>
            <Button
              variant="destructive"
              className="inline-flex items-center gap-2"
              onClick={() => {
                localStorage.clear();
                navigate("/");
              }}
            >
              <FaSignOutAlt /> Logout
            </Button>
          </div>
        </div>

        <Card className="glass-panel">
          <CardHeader className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <CardTitle>All customers</CardTitle>
              <CardDescription>Tap a row to jump straight into a dashboard.</CardDescription>
            </div>
            <Badge variant="outline">Total users · {users.length}</Badge>
          </CardHeader>
          <CardContent>
            <ScrollArea className="max-h-[520px]">
              <table className="w-full border-collapse text-left text-sm">
                <thead>
                  <tr className="border-b border-slate-200 text-xs uppercase tracking-widest text-muted-foreground">
                    <th className="py-3">User ID</th>
                    <th className="py-3">Name</th>
                    <th className="py-3">Phone</th>
                    <th className="py-3 text-right">Actions</th>
                  </tr>
                </thead>

                <tbody>
                  {users.map((u) => (
                    <tr
                      key={u.user_id}
                      className="cursor-pointer border-b border-slate-100 transition hover:bg-slate-50/70"
                      onClick={() => navigate(`/users/${u.user_id}/dashboard`)}
                    >
                      <td className="py-4 font-medium text-slate-900">
                        {u.user_id}
                      </td>
                      <td className="py-4">{u.name}</td>
                      <td className="py-4 text-muted-foreground">{u.phone}</td>
                      <td className="py-4 text-right">
                        <Button
                          variant="ghost"
                          size="sm"
                          className="text-red-500 hover:bg-red-50"
                          onClick={(e) => {
                            e.stopPropagation();
                            handleDelete(u.user_id);
                          }}
                        >
                          <FaTrash />
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </ScrollArea>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
