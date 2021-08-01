#![allow(clippy::upper_case_acronyms)]
#![allow(non_camel_case_types)]

use bitflags::*;
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use failure::*;
use memmap::Mmap;
use std::fs::File;
use std::io::{Cursor, Read};

pub mod mdf;

type Result<T> = std::result::Result<T, failure::Error>;

bitflags! {
    pub struct TapeAttrs: u32 {
        const SOFT_FILE_MARK = 1 << 0;
        const MEDIA_LABEL = 1 << 1;
        const UNKNOWN1 = 1 << 2;
    }
}

#[derive(Debug, Clone)]
pub struct SoftFileMarkBlockSize {
    blocks: u16,
}

impl SoftFileMarkBlockSize {
    fn parse(blocks: u16) -> SoftFileMarkBlockSize {
        SoftFileMarkBlockSize { blocks }
    }

    fn bytes(&self) -> u64 {
        512 * (self.blocks as u64)
    }
}

#[derive(Debug, Clone)]
pub enum MediaBasedCatalogType {
    NONE,
    TYPE_1,
    TYPE_2,
    UNKNOWN3,
}

impl MediaBasedCatalogType {
    fn parse(ty: u16) -> Result<MediaBasedCatalogType> {
        use MediaBasedCatalogType::*;

        match ty {
            0 => Ok(NONE),
            1 => Ok(TYPE_1),
            2 => Ok(TYPE_2),
            3 => Ok(UNKNOWN3),
            _ => Err(format_err!("invalid media based catalog type {}", ty)),
        }
    }
}

bitflags! {
    pub struct SSetAttrs: u32 {
        const TRANSFER = 1 << 0;
        const COPY = 1 << 1;
        const NORMAL = 1 << 2;
        const DIFFERENTIAL = 1 << 3;
        const INCREMENTAL = 1 << 4;
        const DAILY = 1 << 5;
    }
}

bitflags! {
    pub struct VolbAttrs: u32 {
        const NO_REDIRECT_RESTORE = 1 << 0;
        const NON_VOLUME = 1 << 1;
        const DEV_DRIVE = 1 << 2;
        const DEV_UNC = 1 << 3;
        const DEV_OS_SPEC = 1 << 4;
        const DEV_VEND_SPEC = 1 << 5;
    }
}

#[derive(Debug, Clone)]
pub enum DBLKSpecific {
    TAPE {
        media_family_id: u32,
        tape_attrs: TapeAttrs,
        media_sequence_number: u16,
        password_encryption_algorithm: u16,
        soft_filemark_block_size: SoftFileMarkBlockSize,
        media_based_catalog_type: MediaBasedCatalogType,
        media_name: Option<String>,
        media_description: Option<String>,
        media_password: Option<String>,
        software_name: Option<String>,
        format_logical_block_size: u16,
        software_vendor_id: u16,
        media_date: DateTime,
        major_version: u8,
    },
    SSET {
        attrs: SSetAttrs,
        password_encryption_algorithm: u16,
        software_compression_algorithm: u16,
        software_vendor_id: u16,
        data_set_number: u16,
        data_set_name: Option<String>,
        data_set_description: Option<String>,
        data_set_password: Option<String>,
        username: Option<String>,
        physical_block_address: u64,
        write_date: DateTime,
        software_major_version: u8,
        software_minor_version: u8,
        timezone: i8,
        minor_version: u8,
        media_catalog_version: u8,
    },
    VOLB {
        attrs: VolbAttrs,
        device_name: Option<String>,
        volume_name: Option<String>,
        machine_name: Option<String>,
        write_date: DateTime,
    },
    DIRB,
    FILE,
    CFIL,
    ESPB,
    ESET,
    EOTM,
    SFMB {
        number_of_entries: u32,
        used_entries: u32,
        entries: Vec<u32>,
    },
    UNKNOWN(String),
}

#[derive(Debug, Clone)]
pub struct DBLK {
    header: CommonBlockHeader,
    body: DBLKSpecific,
}

#[derive(Debug, Clone)]
pub enum DBLKType {
    TAPE,
    SSET,
    VOLB,
    DIRB,
    FILE,
    CFIL,
    ESPB,
    ESET,
    EOTM,
    SFMB,
    UNKNOWN,
}

impl DBLKType {
    fn parse(ty: u32) -> DBLKType {
        use DBLKType::*;

        match ty {
            0x45504154 => {
                // TAPE
                TAPE
            }
            0x54455353 => {
                // SSET
                SSET
            }
            0x424C4F56 => {
                // VOLB
                VOLB
            }
            0x42524944 => {
                // DIRB
                DIRB
            }
            0x454C4946 => {
                // FILE
                FILE
            }
            0x4C494643 => {
                // CFIL
                CFIL
            }
            0x42505345 => {
                // ESPB
                ESPB
            }
            0x54455345 => {
                // ESET
                ESET
            }
            0x4D544F45 => {
                // EOTM
                EOTM
            }
            0x424D4653 => {
                // SFMB
                SFMB
            }
            _ => {
                UNKNOWN
                // panic!("invalid dblock type 0x{:x}", ty),
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum StringType {
    NO_STRINGS,
    ANSI_STR,
    UNICODE_STR,
}

impl StringType {
    fn parse(ty: u8) -> Result<StringType> {
        use StringType::*;

        match ty {
            0 => Ok(NO_STRINGS),
            1 => Ok(ANSI_STR),
            2 => Ok(UNICODE_STR),
            _ => Err(format_err!("invalid string type {}", ty)),
        }
    }

    fn bytes_to_string(&self, data: Vec<u8>) -> Result<String> {
        use StringType::*;

        match self {
            NO_STRINGS => Err(format_err!(
                "string type was set to NO_STRINGS, but wanted to convert {:#?} to a string",
                data
            )),
            ANSI_STR => Ok(String::from_utf8(data)?),
            UNICODE_STR => Ok(String::from_utf16(
                &data
                    .chunks_exact(2)
                    .into_iter()
                    .map(|a| u16::from_le_bytes([a[0], a[1]]))
                    .collect::<Vec<_>>(),
            )?),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DBLKSets {
    tape: Option<DBLK>,
    set: Option<DBLK>,
    vol: Option<DBLK>,
    dir: Option<DBLK>,
    file: Option<DBLK>,
    soft_mark: Option<DBLK>,
}

impl DBLKSets {
    fn update(&mut self, dblk: DBLK) {
        use DBLKSpecific::*;

        match dblk.body {
            TAPE { .. } => self.tape = Some(dblk),
            SSET { .. } => self.set = Some(dblk),
            VOLB { .. } => self.vol = Some(dblk),
            DIRB { .. } => self.dir = Some(dblk),
            FILE { .. } => self.file = Some(dblk),
            SFMB { .. } => self.soft_mark = Some(dblk),
            _ => {}
        }
    }
}

#[derive(Debug)]
pub struct MTFParser {
    file: File,
    sets: DBLKSets,
    mmap: Option<Mmap>,
}

#[derive(Debug)]
pub struct DBLKWithStreams<'a> {
    pub dblk: DBLK,
    pub streams: Vec<StreamWithData<'a>>,
}

impl<'a> DBLKWithStreams<'a> {
    fn parse<C: AsRef<[u8]>>(cursor: &mut Cursor<C>, sets: &mut DBLKSets, data: &'a [u8]) -> Self {
        let dblk_position = cursor.position();
        let dblock = DBLK::parse(cursor, sets).unwrap();
        sets.update(dblock.clone());

        // all dblck's have atleast the SPAD stream
        cursor.set_position(dblk_position + (dblock.header.offset_to_first_event as u64));

        let streams = StreamWithData::parse_all(cursor, data);

        Self {
            dblk: dblock,
            streams,
        }
    }
}

#[derive(Debug)]
pub struct StreamWithData<'a> {
    pub stream: Stream,
    pub data: &'a [u8],
}

impl<'a> StreamWithData<'a> {
    fn parse_all<C: AsRef<[u8]>>(cursor: &mut Cursor<C>, data: &'a [u8]) -> Vec<Self> {
        Stream::parse_all(cursor)
            .unwrap()
            .into_iter()
            .map(|stream| StreamWithData::from_stream(stream, data))
            .collect()
    }

    fn from_stream(stream: Stream, data: &'a [u8]) -> Self {
        StreamWithData {
            data: stream.data(data),
            stream,
        }
    }
}

pub struct DBLKIterator<'a> {
    sets: &'a mut DBLKSets,
    mmap: &'a Mmap,
    position: u64,
}

impl<'a> DBLKIterator<'a> {
    fn new(sets: &'a mut DBLKSets, mmap: &'a Mmap) -> Self {
        Self {
            sets,
            mmap,
            position: 0,
        }
    }
}

impl<'a> Iterator for DBLKIterator<'a> {
    type Item = DBLKWithStreams<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut cursor = Cursor::new(self.mmap);
        // We would like to save the cursor, but self referential stuff is hard...
        // So we just save the position and then recreate the Cursor...
        cursor.set_position(self.position);

        let dblk = DBLKWithStreams::parse(&mut cursor, self.sets, self.mmap);

        // we don't really have proper detection when the file ends, so for now try to parse the next block
        // and the cursor will prevent going further than the bounds, so just look if we did not move
        if self.position != cursor.position() {
            self.position = cursor.position();

            Some(dblk)
        } else {
            None
        }
    }
}

impl MTFParser {
    pub fn new(filename: &str) -> MTFParser {
        MTFParser {
            file: File::open(filename).unwrap(),
            mmap: None,
            sets: DBLKSets {
                tape: None,
                set: None,
                vol: None,
                dir: None,
                file: None,
                soft_mark: None,
            },
        }
    }

    pub fn dblks(&mut self) -> impl Iterator<Item = DBLKWithStreams> {
        if self.mmap.is_none() {
            self.mmap = Some(unsafe { Mmap::map(&self.file).unwrap() });
        }
        let mmap = self.mmap.as_ref().unwrap();
        DBLKIterator::new(&mut self.sets, &mmap)
    }
}

impl DBLK {
    fn parse<T: AsRef<[u8]>>(data: &mut Cursor<T>, sets: &DBLKSets) -> Result<DBLK> {
        let base = data.position();

        let mut header_data = [0; 52];
        data.read_exact(&mut header_data)?;

        let mut header_data = Cursor::new(&header_data[..]);

        // calculate the checksum
        let mut checksum = 0;
        let mut word = 0;

        while let Ok(new_word) = header_data.read_u16::<LittleEndian>() {
            checksum ^= word;
            word = new_word;
        }

        header_data.set_position(0);

        let id = header_data.read_u32::<LittleEndian>()?;
        let ty = DBLKType::parse(id);
        let attrs = CommonBlockAttrs::parse(header_data.read_u32::<LittleEndian>()?, &ty)?;

        let offset_to_first_event = header_data.read_u16::<LittleEndian>()?;

        let osid = OS::parse(header_data.read_u8()?);
        let osver = header_data.read_u8()?;
        let display_size = header_data.read_u64::<LittleEndian>()?;
        let format_logical_address = header_data.read_u64::<LittleEndian>()?;
        let _reserved_for_mbc = header_data.read_u16::<LittleEndian>()?;

        let mut reserved1 = [0; 6];
        header_data.read_exact(&mut reserved1)?;

        let control_block_id = header_data.read_u32::<LittleEndian>()?;

        let mut reserved2 = [0; 4];
        header_data.read_exact(&mut reserved2)?;

        let os_specific_data = TapeAddress::parse(header_data.read_u32::<LittleEndian>()?, base)?;
        let string_type = StringType::parse(header_data.read_u8()?)?;

        let mut reserved3 = [0; 1];
        header_data.read_exact(&mut reserved3)?;

        let header_checksum = header_data.read_u16::<LittleEndian>()?;

        assert_eq!(
            header_checksum, checksum,
            "got checksum {:#b}, calculated checksum {:#b}",
            header_checksum, checksum
        );

        let header = CommonBlockHeader {
            attrs,
            offset_to_first_event,
            osid,
            osver,
            display_size,
            format_logical_address,
            control_block_id,
            os_specific_data,
            string_type,
            header_checksum,
        };

        let body = match ty {
            DBLKType::TAPE => {
                assert_eq!(
                    format_logical_address, 0,
                    "format_logical_address has to be set to zero for TAPE DBLK, not {}",
                    format_logical_address
                );

                assert_eq!(
                    control_block_id, 0,
                    "control_block_id has to be set to zero for TAPE DBLK, not {}",
                    control_block_id
                );

                let media_family_id = data.read_u32::<LittleEndian>()?;
                let tape_attrs = data.read_u32::<LittleEndian>()?;
                let tape_attrs = TapeAttrs::from_bits(tape_attrs).ok_or_else(|| {
                    format_err!("could not parse tape attributes from {:#b}", tape_attrs)
                })?;
                let media_sequence_number = data.read_u16::<LittleEndian>()?;
                let password_encryption_algorithm = data.read_u16::<LittleEndian>()?;
                let soft_filemark_block_size =
                    SoftFileMarkBlockSize::parse(data.read_u16::<LittleEndian>()?);
                let media_based_catalog_type =
                    MediaBasedCatalogType::parse(data.read_u16::<LittleEndian>()?)?;
                let media_name = TapeAddress::parse(data.read_u32::<LittleEndian>()?, base)?
                    .read_str(&header.string_type, data)?;
                let media_description = TapeAddress::parse(data.read_u32::<LittleEndian>()?, base)?
                    .read_str(&header.string_type, data)?;
                let media_password = TapeAddress::parse(data.read_u32::<LittleEndian>()?, base)?
                    .read_str(&header.string_type, data)?;
                let software_name = TapeAddress::parse(data.read_u32::<LittleEndian>()?, base)?
                    .read_str(&header.string_type, data)?;
                let format_logical_block_size = data.read_u16::<LittleEndian>()?;
                let software_vendor_id = data.read_u16::<LittleEndian>()?;

                let mut media_date = [0; 5];
                data.read_exact(&mut media_date)?;

                let media_date = DateTime::parse(media_date);
                let major_version = data.read_u8()?;

                DBLKSpecific::TAPE {
                    media_family_id,
                    tape_attrs,
                    media_sequence_number,
                    password_encryption_algorithm,
                    soft_filemark_block_size,
                    media_based_catalog_type,
                    media_name,
                    media_description,
                    media_password,
                    software_name,
                    format_logical_block_size,
                    software_vendor_id,
                    media_date,
                    major_version,
                }
            }
            DBLKType::SSET => {
                let attrs = data.read_u32::<LittleEndian>()?;
                let attrs = SSetAttrs::from_bits(attrs).ok_or_else(|| {
                    format_err!("could not parse sset attributes from {:#b}", attrs)
                })?;

                let password_encryption_algorithm = data.read_u16::<LittleEndian>()?;
                let software_compression_algorithm = data.read_u16::<LittleEndian>()?;
                let software_vendor_id = data.read_u16::<LittleEndian>()?;
                let data_set_number = data.read_u16::<LittleEndian>()?;
                let data_set_name = TapeAddress::parse(data.read_u32::<LittleEndian>()?, base)?
                    .read_str(&header.string_type, data)?;
                let data_set_description =
                    TapeAddress::parse(data.read_u32::<LittleEndian>()?, base)?
                        .read_str(&header.string_type, data)?;
                let data_set_password = TapeAddress::parse(data.read_u32::<LittleEndian>()?, base)?
                    .read_str(&header.string_type, data)?;
                let username = TapeAddress::parse(data.read_u32::<LittleEndian>()?, base)?
                    .read_str(&header.string_type, data)?;
                let physical_block_address = data.read_u64::<LittleEndian>()?;

                let mut write_date = [0; 5];
                data.read_exact(&mut write_date)?;
                let write_date = DateTime::parse(write_date);

                let software_major_version = data.read_u8()?;
                let software_minor_version = data.read_u8()?;
                let timezone = data.read_i8()?;
                let minor_version = data.read_u8()?;
                let media_catalog_version = data.read_u8()?;

                DBLKSpecific::SSET {
                    attrs,
                    password_encryption_algorithm,
                    software_compression_algorithm,
                    software_vendor_id,
                    data_set_number,
                    data_set_name,
                    data_set_description,
                    data_set_password,
                    username,
                    physical_block_address,
                    write_date,
                    software_major_version,
                    software_minor_version,
                    timezone,
                    minor_version,
                    media_catalog_version,
                }
            }
            DBLKType::VOLB => {
                let attrs = data.read_u32::<LittleEndian>()?;
                let attrs = VolbAttrs::from_bits(attrs).ok_or_else(|| {
                    format_err!("could not parse volb attributes from {:#b}", attrs)
                })?;

                let device_name = TapeAddress::parse(data.read_u32::<LittleEndian>()?, base)?
                    .read_str(&header.string_type, data)?;
                let volume_name = TapeAddress::parse(data.read_u32::<LittleEndian>()?, base)?
                    .read_str(&header.string_type, data)?;
                let machine_name = TapeAddress::parse(data.read_u32::<LittleEndian>()?, base)?
                    .read_str(&header.string_type, data)?;

                let mut write_date = [0; 5];
                data.read_exact(&mut write_date)?;
                let write_date = DateTime::parse(write_date);

                DBLKSpecific::VOLB {
                    attrs,
                    device_name,
                    volume_name,
                    machine_name,
                    write_date,
                }
            }
            DBLKType::DIRB => {
                // DIRB
                unimplemented!()
            }
            DBLKType::FILE => {
                // FILE
                unimplemented!()
            }
            DBLKType::CFIL => {
                // CFIL
                unimplemented!()
            }
            DBLKType::ESPB => {
                // ESPB
                unimplemented!()
            }
            DBLKType::ESET => {
                // ESET
                unimplemented!()
            }
            DBLKType::EOTM => {
                // EOTM
                unimplemented!()
            }
            DBLKType::SFMB => {
                let number_of_entries = data.read_u32::<LittleEndian>()?;
                let used_entries = data.read_u32::<LittleEndian>()?;

                let soft_filemark_block_size = match sets.tape {
                    Some(ref tape) => {
                        match &tape.body {
                            DBLKSpecific::TAPE { soft_filemark_block_size, .. } => {
                                Ok(soft_filemark_block_size)
                            },
                            _ => {
                                Err(format_err!("tape set was wrong type {:#?}", sets.tape))
                            }
                        }
                    },
                    None => {
                        Err(format_err!("need to have parsed tape dblk to parse sfmb dblk (for soft_filemark_block_size)"))
                    }
                }?;

                // 60 = sizeof(common header = 52) + 2 * u32
                let mut entries_data = vec![0u8; (soft_filemark_block_size.bytes() - 60) as usize];
                let mut entries =
                    vec![0u32; ((soft_filemark_block_size.bytes() - 60) / 4) as usize];

                data.read_exact(&mut entries_data)?;

                LittleEndian::read_u32_into(&entries_data, &mut entries);

                DBLKSpecific::SFMB {
                    number_of_entries,
                    used_entries,
                    entries,
                }
            }
            _ => {
                let id = String::from_utf8(id.to_le_bytes().to_vec())?;
                DBLKSpecific::UNKNOWN(id)
                // panic!("invalid dblock type {:?}", ty),
            }
        };

        Ok(DBLK { header, body })
    }
}

#[derive(Debug, Clone)]
pub enum CommonBlockAttrs {
    ANY(CommonBlockAttrsAny),
    TAPE(CommonBlockAttrsTAPE),
    SSET(CommonBlockAttrsSSET),
    ESET(CommonBlockAttrsESET),
    EOTM(CommonBlockAttrsEOTM),
}

impl CommonBlockAttrs {
    fn parse(attrs: u32, ty: &DBLKType) -> Result<CommonBlockAttrs> {
        use DBLKType::*;

        Ok(match ty {
            TAPE => {
                CommonBlockAttrs::TAPE(CommonBlockAttrsTAPE::from_bits(attrs).ok_or_else(|| {
                    format_err!(
                        "could not parse common block attrs from {:#b} for type {:?}",
                        attrs,
                        ty
                    )
                })?)
            }
            SSET => {
                CommonBlockAttrs::SSET(CommonBlockAttrsSSET::from_bits(attrs).ok_or_else(|| {
                    format_err!(
                        "could not parse common block attrs from {:#b} for type {:?}",
                        attrs,
                        ty
                    )
                })?)
            }
            ESET => {
                CommonBlockAttrs::ESET(CommonBlockAttrsESET::from_bits(attrs).ok_or_else(|| {
                    format_err!(
                        "could not parse common block attrs from {:#b} for type {:?}",
                        attrs,
                        ty
                    )
                })?)
            }
            EOTM => {
                CommonBlockAttrs::EOTM(CommonBlockAttrsEOTM::from_bits(attrs).ok_or_else(|| {
                    format_err!(
                        "could not parse common block attrs from {:#b} for type {:?}",
                        attrs,
                        ty
                    )
                })?)
            }
            _ => CommonBlockAttrs::ANY(CommonBlockAttrsAny::from_bits(attrs).ok_or_else(|| {
                format_err!(
                    "could not parse common block attrs from {:#b} for type {:?}",
                    attrs,
                    ty
                )
            })?),
        })
    }
}

bitflags! {
    pub struct CommonBlockAttrsAny: u32 {
        const CONTINUATION = 1 << 0;
        const COMPRESSION = 1 << 2;
        const EOS_AT_EOM = 1 << 3;
    }
}

bitflags! {
    pub struct CommonBlockAttrsTAPE: u32 {
        const CONTINUATION = 1 << 0;
        const COMPRESSION = 1 << 2;
        const EOS_AT_EOM = 1 << 3;

        const SET_MAP_EXISTS = 1 << 16; // TAPE
        const FDD_ALLOWED = 1 << 17;    // TAPE
    }
}

bitflags! {
    pub struct CommonBlockAttrsSSET: u32 {
        const CONTINUATION = 1 << 0;
        const COMPRESSION = 1 << 2;
        const EOS_AT_EOM = 1 << 3;

        const FDD_EXISTS = 1 << 16; // SSET
        const ENCRYPTION = 1 << 17; // SSET
    }
}

bitflags! {
    pub struct CommonBlockAttrsESET: u32 {
        const CONTINUATION = 1 << 0;
        const COMPRESSION = 1 << 2;
        const EOS_AT_EOM = 1 << 3;

        const FDD_ABORTED = 1 << 16; // ESET
        const END_OF_FAMILY = 1 << 17; // ESET
        const ABORTED_SET = 1 << 18; // ESET
    }
}

bitflags! {
    pub struct CommonBlockAttrsEOTM: u32 {
        const CONTINUATION = 1 << 0;
        const COMPRESSION = 1 << 2;
        const EOS_AT_EOM = 1 << 3;

        const NO_ESET_PBA = 1 << 16; // ESET
        const INVALID_ESET_PBA = 1 << 17; // ESET
    }
}

#[derive(Debug, Clone)]
pub enum OS {
    NetWare,
    NetWareSMS,
    WindowsNT,
    DOS_Windows3_X,
    OS2,
    Windows95,
    Macintosh,
    Unix,
    ToBeAssigned,
    VendorSpecific,
}

impl OS {
    fn parse(os: u8) -> OS {
        use OS::*;

        match os {
            1 => NetWare,
            13 => NetWareSMS,
            14 => WindowsNT,
            24 => DOS_Windows3_X,
            25 => OS2,
            26 => Windows95,
            27 => Macintosh,
            28 => Unix,
            33..=127 => ToBeAssigned,
            _ => VendorSpecific,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TapeAddress {
    size: u16,
    offset: u16,
    base: u64,
}

impl TapeAddress {
    fn parse(tape_address: u32, base: u64) -> Result<TapeAddress> {
        let mut data = Cursor::new(tape_address.to_le_bytes());

        let size = data.read_u16::<LittleEndian>()?;
        let offset = data.read_u16::<LittleEndian>()?;

        Ok(TapeAddress { size, offset, base })
    }

    fn read_str<T: AsRef<[u8]>>(
        self,
        ty: &StringType,
        data: &mut Cursor<T>,
    ) -> Result<Option<String>> {
        if self.size > 0 {
            let old_position = data.position();
            data.set_position(self.base + (self.offset as u64));

            let mut str_data = vec![0; self.size as usize];
            data.read_exact(&mut str_data)?;

            data.set_position(old_position);

            ty.bytes_to_string(str_data).map(Option::Some)
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Clone)]
pub struct DateTime {
    year: u16,
    month: u16,
    day: u16,
    hour: u16,
    minute: u16,
    second: u16,
}

impl DateTime {
    fn parse(data: [u8; 5]) -> DateTime {
        // 40 bits packed format

        let mut wide_data = [0; 5];

        for (i, d) in data.iter().enumerate() {
            wide_data[i] = *d as u16;
        }

        let data = wide_data;

        let year = (data[0] << 6) | (data[1] >> 2);
        let month = ((data[1] & 0b11) << 2) | (data[2] >> 6);
        let day = (data[2] >> 1) & 0b11111;
        let hour = ((data[2] & 0b1) << 4) | (data[3] >> 4);
        let minute = ((data[3] & 0b1111) << 2) | (data[4] >> 6);
        let second = data[4] & 0b111111;

        DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CommonBlockHeader {
    attrs: CommonBlockAttrs,
    offset_to_first_event: u16,
    osid: OS,
    osver: u8,
    display_size: u64,
    format_logical_address: u64,
    control_block_id: u32,
    os_specific_data: TapeAddress,
    string_type: StringType,
    header_checksum: u16,
}

bitflags! {
    pub struct FileSystemAttributes: u16 {
        const MODIFIED_BY_READ = 1 << 0;
        const CONTAINS_SECURITY = 1 << 1;
        const IS_NON_PORTABLE = 1 << 2;
        const IS_SPARSE = 1 << 3;
    }
}

bitflags! {
    pub struct MediaFormatAttributes: u16 {
        const CONTINUE = 1 << 0;
        const VARIABLE = 1 << 1;
        const VAR_END = 1 << 2;
        const ENCRYPTED = 1 << 3;
        const COMRESSED = 1 << 4;
        const CHECKSUMED = 1 << 5;
        const EMBEDDED_LENGTH = 1 << 6;
        const UNKNOWN7 = 1 << 7;
        const UNKNOWN8 = 1 << 8;
    }
}

/*
enum EncryptionAlgorithm {

}

enum CompressionAlgorithm {

}
*/

#[derive(Debug, Clone)]
pub struct StreamHeader {
    pub id: String,
    file_system_attributes: FileSystemAttributes,
    media_format_attributes: MediaFormatAttributes,
    length: u64,
    encryption_algorithm: u16,
    compression_algorithm: u16,
}

#[derive(Debug, Clone)]
pub struct Stream {
    pub header: StreamHeader,
    base: u64,
}

impl Stream {
    fn parse<T: AsRef<[u8]>>(data: &mut Cursor<T>) -> Result<Option<Stream>> {
        let orig = data.position();
        let mut header_data = [0; 22];
        data.read_exact(&mut header_data)?;

        let base = data.position();

        let mut header_data = Cursor::new(&header_data[..]);

        // calculate the checksum
        let mut checksum = 0;
        let mut word = 0;

        while let Ok(new_word) = header_data.read_u16::<LittleEndian>() {
            checksum ^= word;
            word = new_word;
        }
        header_data.set_position(0);

        let id = String::from_utf8(
            header_data
                .read_u32::<LittleEndian>()?
                .to_le_bytes()
                .to_vec(),
        )?;

        // support deprecated mtf's where not all segments have a stream
        match &*id {
            "TAPE" | "SSET" | "VOLB" | "DIRB" | "FILE" | "CFIL" | "ESPB" | "ESET" | "EOTM"
            | "SFMB" => {
                data.set_position(orig);
                return Ok(None);
            }
            _ => {}
        }

        let file_system_attributes = header_data.read_u16::<LittleEndian>()?;
        let file_system_attributes = FileSystemAttributes::from_bits(file_system_attributes)
            .ok_or_else(|| {
                format_err!(
                    "Could not parse stream header file system attributes from {:#b}",
                    file_system_attributes
                )
            })?;

        let media_format_attributes = header_data.read_u16::<LittleEndian>()?;
        let media_format_attributes = MediaFormatAttributes::from_bits(media_format_attributes)
            .ok_or_else(|| {
                format_err!(
                    "Could not parse stream header media format attributes from {:#b}",
                    media_format_attributes
                )
            })?;

        let length = header_data.read_u64::<LittleEndian>()?;

        let encryption_algorithm = header_data.read_u16::<LittleEndian>()?;
        let compression_algorithm = header_data.read_u16::<LittleEndian>()?;

        let header_checksum = header_data.read_u16::<LittleEndian>()?;

        let header = StreamHeader {
            id,
            file_system_attributes,
            media_format_attributes,
            length,
            encryption_algorithm,
            compression_algorithm,
        };

        assert_eq!(
            header_checksum, checksum,
            "got checksum {:#b}, calculated checksum {:#b}",
            header_checksum, checksum
        );

        Ok(Some(Stream { header, base }))
    }

    fn parse_all<T: AsRef<[u8]>>(data: &mut Cursor<T>) -> Result<Vec<Stream>> {
        let mut streams = Vec::new();

        loop {
            let new_stream = Stream::parse(data)?;

            if let Some(new_stream) = new_stream {
                let old_position = data.position();

                let new = old_position + new_stream.header.length;
                let left_over = new % 4;
                let padding = if left_over > 0 { 4 - left_over } else { 0 };

                if new + padding == old_position {
                    // We wont progress from here
                    break;
                }

                data.set_position(new + padding);
                if data.position() != (new + padding) {
                    // Seems like we found the end of the file?
                    break;
                }

                let id = new_stream.header.id.clone();

                streams.push(new_stream);

                if id == "SPAD" {
                    break;
                }
            } else {
                return Ok(Vec::new());
            }
        }

        Ok(streams)
    }

    pub fn data<'a>(&self, data: &'a [u8]) -> &'a [u8] {
        let start = self.base as usize;
        let end = (self.base + self.header.length) as usize;
        let end = data.len().min(end);
        &data[start..end]
    }

    pub fn read<T: AsRef<[u8]>>(&self, data: &mut Cursor<T>) -> Result<Vec<u8>> {
        let old_position = data.position();

        data.set_position(self.base);

        let mut stream_data = vec![0u8; self.header.length as usize];
        data.read_exact(&mut stream_data)?;

        data.set_position(old_position);

        Ok(stream_data)
    }
}
