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
  private static int linkRead, srcRead, dstRead;
  private static int linkLimit, srcLimit, dstLimit;
  private static int linkMemGiven, srcMemGiven, dstMemGiven;
  private static int numAllo = 0;

  private static void pageRankIter() {
    try {
      fileSetUp();
      int linkSize = (int)linkIn.size();
      link.getInt(); //discard the total page num
      int currPage = -1, currLink = -1;
      int linkDst;
      double score = 0;

      while(linkRead < linkSize) {
        while(link.hasRemaining()) {
          if (currPage == -1) {
            currPage = link.getInt();
            if (currPage % 100 ==0) System.out.println(currPage);
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
        linkMem = Math.min(linkMemGiven, linkLimit - linkRead);
        link = linkIn.map(FileChannel.MapMode.READ_ONLY, linkRead, linkMem);
        checkGC();
        linkRead += linkMem;
      }
  
      linkIn.close();
      srcIn.close();
      dstIn.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void checkGC(){
    if (numAllo == 20000) {
      System.gc();
      numAllo = 0;
    } else {
      numAllo ++;
    }
  }

  private static void fileSetUp() {
    try {
      linkIn = new FileInputStream(LINK).getChannel();
      srcIn = new FileInputStream(SRC).getChannel();
      RandomAccessFile dstFile = new RandomAccessFile(DST, "rw");
      dstIn = dstFile.getChannel(); 

      srcMem = srcRead = Math.min(srcMemGiven, srcLimit);
      linkMem = linkRead = Math.min(linkMemGiven, linkLimit);
      dstMem = dstRead = Math.min(dstMemGiven, dstLimit);

      link = linkIn.map(FileChannel.MapMode.READ_ONLY, 0, linkMem);
      src = srcIn.map(FileChannel.MapMode.READ_ONLY, 0, srcMem);
      dst = dstIn.map(FileChannel.MapMode.READ_WRITE, 0, dstMem);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static double getScore(int srcPage, int currLink) {
    try {
      int srcLoc = srcPage * 8;
      if (srcLoc < srcRead - srcMem || srcLoc >= srcRead) {
        srcMem = Math.min(srcMemGiven, srcLimit - srcLoc);
        src = srcIn.map(FileChannel.MapMode.READ_ONLY, srcLoc, srcMem);
        checkGC();
        srcRead = srcLoc + srcMem;
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
      if (dstLoc < dstRead - dstMem || dstLoc >= dstRead) {
        dstMem = Math.min(dstMemGiven, dstLimit - dstLoc);
        dst = dstIn.map(FileChannel.MapMode.READ_WRITE, dstLoc, dstMem);
        checkGC();
        dstRead = dstLoc + dstMem;
      }
      int pos = dstLoc - dstRead + dstMem;
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
    srcMemGiven = Integer.parseInt(args[1]) * 1000000;
    linkMemGiven = Integer.parseInt(args[2]) * 1000000;
    dstMemGiven = Integer.parseInt(args[3]) * 1000000;

    long start, end, iterStart;
    start = System.currentTimeMillis();
    initSrc();
    end = System.currentTimeMillis();
    System.out.println("Init src took "+ (end - start) + " ms");

    srcLimit = dstLimit = (int)new File(SRC).length();
    linkLimit = (int)new File(LINK).length();

    for (int i=0; i<iter; i++) {
      iterStart = System.currentTimeMillis();
      initDst();
      pageRankIter();
      cleanUp();
      end = System.currentTimeMillis();
      System.out.println("Iter " + i + " took "+ (end - iterStart) + " ms");
    }

    System.out.println(iter + " iter " + srcMemGiven / 1000000 + " MB src " + linkMemGiven / 1000000 + 
      " MB link " + dstMemGiven / 1000000 + " MB dst took " + (end - start) + " ms");
  }
}