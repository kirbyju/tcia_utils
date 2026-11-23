# 4.0.0a19 preview

The downloads and examples extras now pin stable tcia-data-retriever 0.1.0.
TDR 0.1.0 promotes the validated 0.1.0rc4 source without engine changes
(8d0d394ef63d94ead01bcdae835c7b61ceb6c196). Completion schema 2 and
aggregate Completed accounting are unchanged.

The tcia_utils v4 interface remains an opt-in preview; this release does not
promote tcia_utils itself to a stable v4 release.

Known issue: reference-output saving behavior is being investigated separately.
The TDR stable promotion does not include the parallel reference-saving fix.

The PathDB developer reported restored slide access on November 23. Both the
CSV URL containing index.php and the canonical URL without it now serve the
notebook sample; byte-range checks returned the same TIFF header.
