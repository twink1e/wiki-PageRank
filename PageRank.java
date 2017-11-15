import java.io.*;
import java.nio.*;
import java.nio.channels.*;

class PageRank {
  private static final String LINK = "links.bin", SRC = "src.bin", DST = "dst.bin";
  private static final double DAMPING = 0.85;
  private static int numPage;
  private static FileChannel linkIn, srcIn, dstIn;
  private static MappedByteBuffer link, src, dst;
  private static int linkMem, srcMem, dstMem;
  private static int linkOffset, srcOffset, dstOffset;

  private static void pageRankIter() {
    try {
      fileSetUp();
      int linkSize = (int)linkIn.size();
      link.getInt(); //discard the total page num
      int currPage = -1, currLink = -1;
      int linkDst;
      double score = 0;

      while(linkOffset < linkSize) {
        while(link.hasRemaining()) {
          if (currPage == -1) {
            currPage = link.getInt();
            System.out.println(currPage);
          } else if (currLink == -1) {
            currLink = link.getInt();
            score = getScore(currPage, currLink);
          } else if (currLink == 0) {
            currLink = currPage = -1;
          } else {
            linkDst = link.getInt();
            addScore(linkDst, score);
            currLink--;
            //System.out.println("page " + currPage + "link " + currLink);
          }
        } 
        link = linkIn.map(FileChannel.MapMode.READ_ONLY, linkOffset, linkMem);
        System.gc();
        linkOffset += linkMem;
      }
  
      linkIn.close();
      srcIn.close();
      dstIn.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void fileSetUp() {
    try {
      linkIn = new FileInputStream(LINK).getChannel();
      srcIn = new FileInputStream(SRC).getChannel();
      RandomAccessFile dstFile = new RandomAccessFile(DST, "rw");
      dstIn = dstFile.getChannel(); 

      link = linkIn.map(FileChannel.MapMode.READ_ONLY, 0, linkMem);
      src = srcIn.map(FileChannel.MapMode.READ_ONLY, 0, srcMem);
      dst = dstIn.map(FileChannel.MapMode.READ_WRITE, 0, dstMem);

      linkOffset = linkMem;
      srcOffset = srcMem;
      dstOffset = dstMem;
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static double getScore(int srcPage, int currLink) {
    try {
      int srcLoc = srcPage * 8;
      if (srcLoc < srcOffset || srcLoc >= (srcOffset + srcMem)) {
        src = srcIn.map(FileChannel.MapMode.READ_ONLY, srcLoc, srcMem);
        System.gc();
        srcOffset = srcLoc;
      }
      // SRC score is accessed sequentially
      double ownScore = src.getDouble();
      if (currLink == 0) return 0;
      else return ownScore/currLink;
    } catch (Exception e) {
      e.printStackTrace();
      return 0;
    }
  }

  private static void addScore(int dstPage, double score) {
    try {
      int dstLoc = dstPage * 8;
      if (dstLoc < dstOffset || dstLoc >= (dstOffset + dstMem)) {
        dst = dstIn.map(FileChannel.MapMode.READ_WRITE, dstLoc, dstMem);
        System.gc();
        dstOffset = dstLoc;
      }
      int pos = dstLoc - dstOffset;
      double oldScore = dst.getDouble(pos);
      dst.putDouble(pos, oldScore + DAMPING * score);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /** All pages have initial score of 1/(number of pages) */
  private static void initSrc() {
    try {
      FileInputStream fis = new FileInputStream(LINK);
      BufferedInputStream bis = new BufferedInputStream(fis);
      DataInputStream dis = new DataInputStream(bis);

      FileOutputStream fos = new FileOutputStream(SRC);
      BufferedOutputStream bos = new BufferedOutputStream(fos);
      DataOutputStream dos = new DataOutputStream(bos);

      numPage = dis.readInt();
      dis.close();
      double score = 1.0/numPage;
      for(int i=0; i<numPage; i++) dos.writeDouble(score);
      dos.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /** Create the dst file filled with (1-DAMPING)/numPage */
  private static void initDst() {
    try {
      FileOutputStream fos = new FileOutputStream(DST);
      BufferedOutputStream bos = new BufferedOutputStream(fos);
      DataOutputStream dos = new DataOutputStream(bos);

      double score = (1 - DAMPING)/numPage;
      for(int i=0; i<numPage; i++) dos.writeDouble(score);
      dos.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /** Delete SRC file, rename DST file to SRC*/
  private static void cleanUp() {
    try {
      File src = new File(SRC);
      File dst = new File(DST);
      src.delete();
      dst.renameTo(src);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main (String[] args) {
    if (args.length != 4) {
      System.out.println("Usage: java PageRank {iteration} {src MB} {link MB} {dst MB}");
      System.exit(-1);
    }

    int iter = Integer.parseInt(args[0]);
    srcMem = Integer.parseInt(args[1]) * 1000000;
    linkMem = Integer.parseInt(args[2]) * 1000000;
    dstMem = Integer.parseInt(args[3]) * 1000000;

    long start, end, iterStart;
    start = System.currentTimeMillis();
    initSrc();
    end = System.currentTimeMillis();
    System.out.println("Init src took "+ (end - start) + " ms");

    for (int i=0; i<iter; i++) {
      iterStart = System.currentTimeMillis();
      initDst();
      pageRankIter();
      cleanUp();
      end = System.currentTimeMillis();
      System.out.println("Iter " + i + " took "+ (end - iterStart) + " ms");
    }

    System.out.println(iter + " iter " + srcMem / 1000000 + " MB src " + linkMem / 1000000 + 
      " MB link " + dstMem / 1000000 + " MB dst took " + (end - start) + " ms");
  }
}