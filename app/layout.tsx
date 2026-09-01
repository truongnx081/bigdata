import type { Metadata } from 'next';
import { Geist, Geist_Mono } from 'next/font/google';
import './globals.css';

const geistSans = Geist({
  variable: '--font-geist-sans',
  subsets: ['latin'],
});

const geistMono = Geist_Mono({
  variable: '--font-geist-mono',
  subsets: ['latin'],
});

export const metadata: Metadata = {
  metadataBase: new URL(process.env.NEXT_PUBLIC_SITE_URL || 'http://localhost:3000'),
  title: 'LiveRoad — Phân tích giao thông từ video',
  description: 'Website demo hiển thị mật độ giao thông, tốc độ trung bình và cảnh báo ùn tắc từ video.',
  openGraph: {
    title: 'LiveRoad — Phân tích giao thông từ video',
    description: 'Website demo hiển thị mật độ giao thông, tốc độ trung bình và cảnh báo ùn tắc từ video.',
    images: [{ url: '/og.png', width: 1792, height: 1024, alt: 'LiveRoad — Phân tích giao thông thời gian thực' }],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'LiveRoad — Phân tích giao thông từ video',
    description: 'Website demo hiển thị mật độ giao thông, tốc độ trung bình và cảnh báo ùn tắc từ video.',
    images: ['/og.png'],
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="vi">
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased`}
      >
        {children}
      </body>
    </html>
  );
}
