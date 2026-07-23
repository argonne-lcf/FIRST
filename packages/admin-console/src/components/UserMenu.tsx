import { useGlobusAuth } from "@globus/react-auth-context";
import { useNavigate } from "@tanstack/react-router";
import { LogOut } from "lucide-react";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Skeleton } from "@/components/ui/skeleton";

function initials(name: string): string {
  const parts = name.trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return "?";
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
  return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
}

export function UserMenu() {
  const { authorization } = useGlobusAuth();
  const navigate = useNavigate();
  const user = authorization?.user ?? null;

  if (!user) {
    return <Skeleton className="size-8 rounded-full" />;
  }

  const name = user.name ?? user.preferred_username ?? "Unknown user";
  const email = user.email ?? user.preferred_username;

  const logout = async () => {
    await authorization?.revoke();
    await navigate({ to: "/", replace: true });
  };

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          variant="ghost"
          size="icon"
          className="group rounded-full ring-offset-2 ring-offset-background transition hover:ring-2 hover:ring-ring focus-visible:ring-2 focus-visible:ring-ring data-[state=open]:ring-2 data-[state=open]:ring-ring"
        >
          <Avatar>
            <AvatarFallback className="transition-colors group-hover:bg-accent">
              {initials(name)}
            </AvatarFallback>
          </Avatar>
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-56">
        <DropdownMenuLabel className="flex flex-col gap-0.5">
          <span className="truncate font-medium">{name}</span>
          {email && email !== name && (
            <span className="truncate text-xs font-normal text-muted-foreground">
              {email}
            </span>
          )}
        </DropdownMenuLabel>
        <DropdownMenuSeparator />
        <DropdownMenuItem onSelect={() => void logout()}>
          <LogOut />
          Log out
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
