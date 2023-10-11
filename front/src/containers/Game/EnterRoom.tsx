import { useState } from "react";
import { useRouter } from "next/navigation";
import type { Room } from "@/types";
import styles from "./game.module.scss";
import Modal from "@/components/gameModal";
import SearchRoomModal from "./SearchRoomModal";
import CreateRoomModal from "./CreateRoomModal";

interface Props {
  rooms: Room[];
}

export default function EnterRoom({ rooms }: Props) {
  const router = useRouter();
  // console.log(rooms);

  // 방 생성 모달
  const [openCreateRoom, setOpenCreateRoom] = useState(false);
  // 방 찾기 모달
  const [openSearchRoom, setOpenSearchRoom] = useState(false);

  /** 빠른 시작 버튼
   *
   * 랜덤으로 Waiting인 방을 찾아서 이동
   * Waiting인 방이 없다면 이동하지 않음
   */
  const randomOnClick = () => {
    const arr: number[] = [];
    rooms.forEach((element: Room) => {
      if (!element.roomStatus) {
        if (element.roomCurrent! < element.roomSize!) {
          arr.push(element.roomId!);
        }
      }
    });
    // console.log(arr);
    if (arr.length === 0) {
      // eslint-disable-next-line no-alert
      alert("입장할 수 있는 방이 없습니다.");
      return undefined;
    }
    const randomIndex = Math.floor(Math.random() * arr.length);
    const randomRoomNum = arr[randomIndex];
    router.push(`/game/start/room/${randomRoomNum}`);
    return 0;
  };

  return (
    <>
      <div>
        <button className={styles.boxButton} onClick={randomOnClick}>
          빠른 시작
        </button>
        <button className={styles.boxButton} onClick={() => setOpenSearchRoom(true)}>
          방 찾기
        </button>
        {openSearchRoom && (
          <Modal isOpen={openSearchRoom}>
            <SearchRoomModal onClose={() => setOpenSearchRoom(false)} />
          </Modal>
        )}
      </div>
      <div />
      <button onClick={() => setOpenCreateRoom(true)} className={styles.boxButton}>
        방 만들기
      </button>
      {openCreateRoom && (
        <Modal isOpen={openCreateRoom}>
          <CreateRoomModal onClose={() => setOpenCreateRoom(false)} />
        </Modal>
      )}
    </>
  );
}
