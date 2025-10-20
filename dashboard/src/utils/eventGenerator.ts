
import { Tower, Event, EventType } from "../types/network";

export const generateRandomEvent = (towers: Tower[]): Event => {
  const eventTypes: EventType[] = [
    "call-placed",
    "call-dropped",
    "tower-up",
    "tower-down",
    "alert-triggered",
    "alert-resolved"
  ];
  
  // Select random tower
  const randomTower = towers[Math.floor(Math.random() * towers.length)];
  
  // For tower status change events, make sure we're changing to a different status
  let eventType = eventTypes[Math.floor(Math.random() * eventTypes.length)];
  
  // Don't change status to what it already is
  if (eventType === "tower-up" && randomTower.status === "up") {
    eventType = "call-placed";
  } else if (eventType === "tower-down" && randomTower.status === "down") {
    eventType = "call-dropped";
  }
  
  // Generate appropriate message for the event
  let message = "";
  switch (eventType) {
    case "call-placed":
      message = `A new call was connected through ${randomTower.name} at ${randomTower.lat.toFixed(2)}, ${randomTower.lng.toFixed(2)}`;
      break;
    case "call-dropped":
      message = `A call was dropped at ${randomTower.name}. Signal strength was low.`;
      break;
    case "tower-up":
      message = `${randomTower.name} is now online and operational.`;
      break;
    case "tower-down":
      message = `${randomTower.name} has gone offline. Maintenance team has been notified.`;
      break;
    case "alert-triggered":
      message = `High traffic alert triggered at ${randomTower.name}. Capacity at 90%.`;
      break;
    case "alert-resolved":
      message = `Traffic levels at ${randomTower.name} have returned to normal.`;
      break;
  }
  
  return {
    type: eventType,
    timestamp: new Date(),
    message,
    towerId: randomTower.id
  };
};
