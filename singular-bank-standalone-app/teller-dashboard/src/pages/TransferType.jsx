import { useNavigate, useParams } from "react-router-dom";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "../components/ui/card";
import { Button } from "../components/ui/button";
import { Badge } from "../components/ui/badge";
import { Separator } from "../components/ui/separator";
import {
  FaExchangeAlt,
  FaUniversity,
  FaArrowRight,
} from "react-icons/fa";

const transferOptions = [
  {
    title: "Internal transfer",
    description:
      "Move balances between checking ↔ savings belonging to this user.",
    accent: "bg-indigo-500/10 text-indigo-600",
    icon: FaExchangeAlt,
    action: "internal",
  },
  {
    title: "Same-branch transfer",
    description: "Send funds to another customer on this branch instantly.",
    accent: "bg-purple-500/10 text-purple-600",
    icon: FaArrowRight,
    action: "same-branch",
  },
  {
    title: "External transfer",
    description: "Schedule disbursements to other banks (coming soon).",
    accent: "bg-emerald-500/10 text-emerald-600",
    icon: FaUniversity,
    action: "external",
    disabled: true,
  },
];

export default function TransferType() {
  const navigate = useNavigate();
  const { userId } = useParams();

  return (
    <div className="relative min-h-screen bg-gradient-to-br from-slate-50 via-white to-slate-100 px-4 py-16">
      <div className="absolute inset-x-0 top-8 mx-auto h-72 max-w-5xl rounded-3xl bg-glass-gradient blur-3xl" />
      <div className="relative mx-auto w-full max-w-3xl">
        <Card className="glass-panel">
          <CardHeader>
            <Badge className="w-fit uppercase tracking-[0.35em]" variant="outline">
              Transfer
            </Badge>
            <CardTitle className="text-3xl">Choose transfer type</CardTitle>
            <p className="text-sm text-muted-foreground">
              Quickly route money to the right destination for user #{userId}.
            </p>
          </CardHeader>

          <CardContent className="space-y-6">
            {transferOptions.map(
              ({ title, description, accent, icon: Icon, action, disabled }) => (
                <div
                  key={action}
                  className="rounded-2xl border border-slate-200/80 bg-white/60 px-4 py-4 shadow-sm backdrop-blur transition hover:-translate-y-[1px] hover:shadow-md"
                >
                  <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
                    <div>
                      <div
                        className={`inline-flex items-center gap-2 rounded-full px-3 py-1 text-xs font-semibold ${accent}`}
                      >
                        <Icon className="h-3.5 w-3.5" />
                        {title}
                      </div>
                      <p className="mt-2 text-sm text-muted-foreground">
                        {description}
                      </p>
                    </div>

                    <Button
                      disabled={disabled}
                      variant={disabled ? "outline" : "default"}
                      className="whitespace-nowrap"
                      onClick={() =>
                        !disabled && navigate(`/users/${userId}/transfer/${action}`)
                      }
                    >
                      {disabled ? "Coming soon" : "Continue"}
                    </Button>
                  </div>
                </div>
              )
            )}

            <Separator />

            <Button
              variant="ghost"
              className="w-full text-muted-foreground"
              onClick={() => navigate(`/users/${userId}/dashboard`)}
            >
              ← Back to Dashboard
            </Button>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
