import {
  Card,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

export function Health() {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Health</CardTitle>
        <CardDescription>
          Gateway health metrics will appear here.
        </CardDescription>
      </CardHeader>
    </Card>
  );
}
