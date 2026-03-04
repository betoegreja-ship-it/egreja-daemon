import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

/**
 * Formata um preço com casas decimais adequadas ao seu tamanho.
 * - Preços >= 1000: 2 casas (ex: $67,123.45)
 * - Preços >= 1: 2 casas (ex: $1.93)
 * - Preços >= 0.01: 4 casas (ex: $0.0934)
 * - Preços >= 0.0001: 6 casas (ex: $0.000594)
 * - Preços menores: 8 casas (ex: $0.00000550)
 */
export function formatPrice(value: number | string | null | undefined, prefix = '$'): string {
  if (value === null || value === undefined || value === '') return `${prefix}0.00`;
  const num = typeof value === 'string' ? parseFloat(value) : value;
  if (isNaN(num)) return `${prefix}0.00`;
  
  let decimals: number;
  const abs = Math.abs(num);
  
  if (abs === 0) {
    decimals = 2;
  } else if (abs >= 1) {
    decimals = 2;
  } else if (abs >= 0.01) {
    decimals = 4;
  } else if (abs >= 0.0001) {
    decimals = 6;
  } else {
    decimals = 8;
  }
  
  return `${prefix}${num.toFixed(decimals)}`;
}

/**
 * Formata um preço sem prefixo de moeda (apenas o número).
 */
export function formatPriceRaw(value: number | string | null | undefined): string {
  if (value === null || value === undefined || value === '') return '0.00';
  const num = typeof value === 'string' ? parseFloat(value) : value;
  if (isNaN(num)) return '0.00';
  
  const abs = Math.abs(num);
  let decimals: number;
  
  if (abs === 0) {
    decimals = 2;
  } else if (abs >= 1) {
    decimals = 2;
  } else if (abs >= 0.01) {
    decimals = 4;
  } else if (abs >= 0.0001) {
    decimals = 6;
  } else {
    decimals = 8;
  }
  
  return num.toFixed(decimals);
}
